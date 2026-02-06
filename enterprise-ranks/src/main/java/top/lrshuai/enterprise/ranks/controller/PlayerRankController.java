package top.lrshuai.enterprise.ranks.controller;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.web.bind.annotation.*;
import top.lrshuai.enterprise.common.resp.R;
import top.lrshuai.enterprise.ranks.service.PlayerRankService;
import top.lrshuai.enterprise.ranks.vo.RankItem;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 玩家排名接口层
 */
@RestController
@RequestMapping("/api/rank")
@RequiredArgsConstructor
public class PlayerRankController {

    private final PlayerRankService playerRankService;
    // 批次
    private final long BATCH_SIZE=10000;
    private final long TOTAL=100000000;

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    // 核心线程池
    private final ExecutorService executor = new ThreadPoolExecutor(
            5, // 核心线程数（初始并发）
            10, // 最大线程数（峰值并发）
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000), // 任务队列，避免线程过多
            new ThreadPoolExecutor.CallerRunsPolicy() // 队列满时，由调用线程执行（避免任务丢失）
    );


    /**
     * 批量生成示例排名数据（生成String类型playerId）
     */
    @GetMapping("/batchAdd")
    public R<String> batchAddPlayerData(
            @RequestParam(defaultValue = "1") int rankType,
            @RequestParam(defaultValue = "100") int totalCount,
            @RequestParam(defaultValue = "0") long beginId, //从什么用户ID开始
            @RequestParam(defaultValue = "100") long minScore,
            @RequestParam(defaultValue = "30000") long maxScore) {
        try {
            if (totalCount <= 0 || minScore > maxScore) {
                return R.fail("参数错误：数量需大于0，最小分数不能大于最大分数");
            }

            long batchNum = totalCount / BATCH_SIZE;
            long remainCount = totalCount % BATCH_SIZE;
            long updateTime = System.currentTimeMillis();

            // 创建CountDownLatch，用于等待所有任务完成
            CountDownLatch latch = new CountDownLatch((int)(batchNum + (remainCount > 0 ? 1 : 0)));

            // 提交分批任务
            for (long batch = 0; batch < batchNum; batch++) {
                long startId = batch * BATCH_SIZE + 1+beginId; // 玩家ID起始值
                long endId = (batch + 1) * BATCH_SIZE+beginId;
                submitBatchTaskWithLatch(rankType, startId, endId, minScore, maxScore, updateTime, latch);
                // 每批提交后稍微停顿一下，避免压力过大
                Thread.sleep(100);
            }

            // 处理剩余数据
            if (remainCount > 0) {
                long startId = batchNum * BATCH_SIZE + 1+beginId;
                long endId = batchNum * BATCH_SIZE + remainCount+beginId;
                submitBatchTaskWithLatch(rankType, startId, endId, minScore, maxScore, updateTime, latch);
            }

            // 等待所有批次任务完成
            boolean allCompleted = latch.await(5, TimeUnit.MINUTES); // 设置5分钟超时
            if (!allCompleted) {
                return R.fail("批量生成任务超时，部分数据可能未完成");
            }

            return R.ok("批量生成" + totalCount + "条排名数据成功，所有任务已完成");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return R.fail("批量添加被中断：" + e.getMessage());
        } catch (Exception e) {
            return R.fail("批量添加失败：" + e.getMessage());
        }
    }

    /**
     * 提交单批插入任务（带同步锁）
     */
    private void submitBatchTaskWithLatch(int rankType, long startId, long endId,
                                          long minScore, long maxScore, long updateTime,
                                          CountDownLatch latch) {
        executor.submit(() -> {
            try {
                for (long i = startId; i <= endId; i++) {
                    // 生成随机主分数
//                    long mainScore = minScore + random.nextLong() % (maxScore - minScore + 1);
                    // 为了方便测试定位，分数和id 有关
                    long mainScore = TOTAL-i;
                    System.out.println("i=" + i + ", mainScore=" + mainScore);
                    // 调用服务插入
                    playerRankService.updatePlayerScore(i, rankType, mainScore, updateTime);
                }
                System.out.println("批次完成：player_" + startId + " ~ player_" + endId);
            } catch (Exception e) {
                System.err.println("批次插入失败：" + startId + "~" + endId + "，原因：" + e.getMessage());
            } finally {
                // 无论成功还是失败，都释放锁
                latch.countDown();
            }
        });
    }

    /**
     * 更新玩家分数（核心接口）
     *
     * @param playerId  玩家ID
     * @param rankType  榜单类型（1=战力榜，2=积分榜）
     * @param mainScore 主分数（如战力值）
     */
    @PostMapping("/update")
    public R<String> updateScore(
            @RequestParam Long playerId,
            @RequestParam int rankType,
            @RequestParam long mainScore) {
        // 用当前时间戳作为次分数，保证同主分数时的排序
        long updateTime = System.currentTimeMillis();
        playerRankService.updatePlayerScore(playerId, rankType, mainScore, updateTime);
        return R.ok("分数更新成功");
    }


    /**
     * 查询榜单前N名玩家
     *
     * @param rankType 榜单类型
     * @param start    起始排名（1开始）
     * @param end      结束排名（1开始）
     */
    @SneakyThrows
    @GetMapping("/top/{rankType}/{start}/{end}")
    public R getTopRankList(@PathVariable int rankType, @PathVariable int start, @PathVariable int end) {
        return R.ok(playerRankService.getTopRankList(rankType, start, end));
    }

    /**
     * 查询榜单前N名玩家-优化版
     *
     * @param rankType 榜单类型
     * @param start    起始排名（1开始）
     * @param end      结束排名（1开始）
     */
    @SneakyThrows
    @GetMapping("/topOptimize/{rankType}/{start}/{end}")
    public R getTopRankListOptimize(@PathVariable int rankType, @PathVariable int start, @PathVariable int end) {
        return R.ok(playerRankService.getTopRankListOptimize(rankType, start, end));
    }

    /**
     * 获取玩家排名详情（包含分数和排名）
     *
     * @param playerId 玩家ID
     * @param rankType 榜单类型
     */
    @SneakyThrows
    @GetMapping("/detail/{rankType}/{playerId}")
    public R getPlayerRankDetail(@PathVariable int rankType,
                                 @PathVariable Long playerId) {
        RankItem rankDetail = playerRankService.getPlayerRankDetail(playerId, rankType);
        if (rankDetail == null) {
            return R.fail("玩家不存在或未参与排名");
        }
        return R.ok(rankDetail);
    }

    /**
     * 获取指定分数的所有玩家
     *
     * @param rankType  榜单类型
     * @param mainScore 主分数
     */
    @SneakyThrows
    @GetMapping("/players-by-score/{rankType}/{mainScore}")
    public R getPlayersByMainScore(@PathVariable int rankType,
                                   @PathVariable long mainScore) {
        List<Long> players = playerRankService.getPlayersByMainScore(rankType, mainScore);
        return R.ok(players);
    }

    /**
     * 批量获取玩家排名信息
     *
     * @param rankType  榜单类型
     * @param playerIds 玩家ID列表，用逗号分隔
     */
    @SneakyThrows
    @GetMapping("/batch-detail/{rankType}")
    public R batchGetRankDetails(@PathVariable int rankType,
                                 @RequestParam String playerIds) {
        List<Long> idList = Arrays.stream(playerIds.split(",")).map(Long::parseLong).collect(Collectors.toList());
        List<RankItem> rankDetails = playerRankService.batchGetRankDetails(idList, rankType);
        return R.ok(rankDetails);
    }

    /**
     * 获取玩家周围的排名（显示玩家及前后几名）
     *
     * @param rankType 榜单类型
     * @param playerId 玩家ID
     * @param before   向前获取几名
     * @param after    向后获取几名
     */
    @SneakyThrows
    @GetMapping("/surrounding/{rankType}/{playerId}")
    public R getSurroundingRanks(@PathVariable int rankType,
                                 @PathVariable Long playerId,
                                 @RequestParam(defaultValue = "5") int before,
                                 @RequestParam(defaultValue = "5") int after) {
        List<RankItem> surrounding = playerRankService.getSurroundingRanks(playerId, rankType, before, after);
        return R.ok(surrounding);
    }

    /**
     * 分页查询排行榜
     *
     * @param rankType 榜单类型
     * @param page     页码（从1开始）
     * @param pageSize 每页大小
     */
    @SneakyThrows
    @GetMapping("/page/{rankType}")
    public R getRankByPage(@PathVariable int rankType,
                           @RequestParam(defaultValue = "1") int page,
                           @RequestParam(defaultValue = "20") int pageSize) {
        if (page < 1 || pageSize < 1 || pageSize > 1000) {
            return R.fail("参数错误：页码需大于0，每页大小在1-1000之间");
        }

        int start = (page - 1) * pageSize + 1;
        int end = page * pageSize;

        List<RankItem> rankList = playerRankService.getTopRankList(rankType, start, end);
        return R.ok(rankList);
    }

    /**
     * 获取排行榜统计信息
     *
     * @param rankType 榜单类型
     */
    @SneakyThrows
    @GetMapping("/stats/{rankType}")
    public R getRankStats(@PathVariable int rankType) {
        // 这里可以返回一些统计信息，比如总玩家数、平均分数等
        // 注意：这些数据可能需要额外计算，这里只是示例
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("rankType", rankType);
        stats.put("timestamp", System.currentTimeMillis());

        // 获取前10名
        List<RankItem> top10 = playerRankService.getTopRankList(rankType, 1, 10);
        stats.put("top10", top10);

        // 获取玩家总数（需要遍历所有分片）
        long totalPlayers = 0;
        for (int i = 0; i < playerRankService.getShardCount(); i++) {
            String shardKey = playerRankService.getRankKeyPrefix() + rankType + ":" + i;
            org.redisson.api.RScoredSortedSet<Long> rankSet = playerRankService.getRedissonClient().getScoredSortedSet(shardKey);
            totalPlayers += rankSet.size();
        }
        stats.put("totalPlayers", totalPlayers);

        return R.ok(stats);
    }


}
