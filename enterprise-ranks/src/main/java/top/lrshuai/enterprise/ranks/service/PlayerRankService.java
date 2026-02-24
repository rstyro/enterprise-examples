package top.lrshuai.enterprise.ranks.service;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import top.lrshuai.enterprise.ranks.vo.RankItem;

import java.util.*;
import java.util.concurrent.*;

/**
 * 玩家排名核心服务
 */
@Slf4j
@Data
@Service
@RequiredArgsConstructor
public class PlayerRankService {

    @Value("${rank.shard-count:100}")
    private int shardCount;

    @Value("${rank.key-prefix:game:player:rank:}")
    private String rankKeyPrefix;

    private final RedissonClient redissonClient;

    // 主分数占用高30位，时间戳占用低34位
    private static final int TIMESTAMP_BITS = 34;   // 最多2^34 = 17,179,869,184
    private static final long TIMESTAMP_MASK = (1L << TIMESTAMP_BITS) - 1;

    private final ExecutorService executor = new ThreadPoolExecutor(
            5, // 核心线程数（初始并发）
            5, // 最大线程数（峰值并发）
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000), // 任务队列，避免线程过多
            new ThreadPoolExecutor.CallerRunsPolicy() // 队列满时，由调用线程执行（避免任务丢失）
    );

    /**
     * 组合分数（解决同分数排序问题,分数相同，后面的更新的得到的分数更低）
     * 将主分数和时间戳组合成一个Long
     * 结构：[主分数30位][时间戳34位]
     */
    public static long combineScore(long mainScore, long updateTime) {
        // 验证分数范围
        if (mainScore < 0 || mainScore > 1_000_000_000) {
            throw new IllegalArgumentException("主分数必须在0-10亿之间: " + mainScore);
        }
        // 取时间戳的低34位,34位时间戳 ≈ 170亿个不同值
        long timestampPart = updateTime & TIMESTAMP_MASK;
        // 反转时间戳，分数相同，后面的更新的得到的组合分数更低
        timestampPart = TIMESTAMP_MASK - timestampPart;
        // 主分数左移34位，时间戳放在低34位
        return (mainScore << TIMESTAMP_BITS) | timestampPart;
    }

    /**
     * 从组合Long中解析主分数
     */
    public static long parseMainScore(double combinedScore) {
        // 右移34位获取主分数
        return (long)combinedScore >>> TIMESTAMP_BITS;
    }


    /**
     * 计算玩家所属的分片Key
     * @param playerId 玩家ID
     * @param rankType 排名类型
     * @return 分片键
     */
    private String getShardKey(Long playerId, int rankType) {
        int shardIndex = (int) (playerId % shardCount);
        return rankKeyPrefix + rankType + ":" + shardIndex;
    }

    /**
     * 获取玩家原始排名（Redis原生排名，同分数按字典序）
     */
    public long getOriginalRank(Long playerId, int rankType) {
        String shardKey = getShardKey(playerId, rankType);
        RScoredSortedSet<Long> rankSet = redissonClient.getScoredSortedSet(shardKey);
        Integer rankIndex = rankSet.revRank(playerId);
        return rankIndex == null ? -1 : rankIndex + 1;
    }

    /**
     * 实时更新玩家分数（原子操作）
     */
    public void updatePlayerScore(Long playerId, int rankType, long mainScore, long updateTime) {
        double combineScore = combineScore(mainScore, updateTime);
        String shardKey = getShardKey(playerId, rankType);
        RScoredSortedSet<Long> rankSet = redissonClient.getScoredSortedSet(shardKey);
        // 原子新增/更新分数
        rankSet.add(combineScore, playerId);
    }

    /**
     * 获取玩家详细信息
     */
    @SneakyThrows
    public RankItem getPlayerRankDetail(Long playerId, int rankType) {
        String shardKey = getShardKey(playerId, rankType);
        RScoredSortedSet<Long> rankSet = redissonClient.getScoredSortedSet(shardKey);
        // 获取玩家排序分数
        Double combineScore = rankSet.getScore(playerId);
        if (combineScore == null) {
            return null;
        }
        // 并行计算所有分片中分数大于当前玩家的数量
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            String currentShardKey = rankKeyPrefix + rankType + ":" + i;
            CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
                RScoredSortedSet<Long> shardSet = redissonClient.getScoredSortedSet(currentShardKey);
                // 统一统计所有分片中分数严格大于当前玩家的数量
                // 参数说明：combineScore=当前分数, startScoreInclusive=false(>), maxScore=最大值, endScoreInclusive=true(≤)
                return shardSet.count(combineScore, false, Double.MAX_VALUE, true);
            });
            futures.add(future);
        }

        // 汇总所有分片的结果
        long higherCount = 0;
        for (CompletableFuture<Integer> future : futures) {
            higherCount += future.get();
        }

        long rank = higherCount + 1;
        long mainScore = parseMainScore(combineScore);
        return new RankItem(playerId, mainScore, rank, combineScore);
    }

    /**
     * 获取指定主分数的所有玩家
     */
    public List<Long> getPlayersByMainScore(int rankType, long mainScore) throws ExecutionException, InterruptedException {
        List<CompletableFuture<List<Long>>> futures = new ArrayList<>();

        // 异步遍历所有分片，避免主线程阻塞
        for (int i = 0; i < shardCount; i++) {
            String shardKey = rankKeyPrefix + rankType + ":" + i;
            CompletableFuture<List<Long>> future = CompletableFuture.supplyAsync(() -> {
                RScoredSortedSet<Long> rankSet = redissonClient.getScoredSortedSet(shardKey);
                // 计算主分数对应的组合分数区间
                double minScore = (double) (mainScore << TIMESTAMP_BITS);          // 区间起点
                double maxScore = (double) (((mainScore + 1) << TIMESTAMP_BITS) - 1); // 区间终点

                // valueRange（包含min和max边界）
                Collection<Long> playerCollection = rankSet.valueRange(minScore, true, maxScore, true);
                return new ArrayList<>(playerCollection); // Collection转List
            });
            futures.add(future);
        }

        // 合并所有分片结果
        List<Long> allPlayers = new ArrayList<>();
        for (CompletableFuture<List<Long>> future : futures) {
            allPlayers.addAll(future.get());
        }
        return allPlayers;
    }

    /**
     * 获取榜单前N名（带分数和排名）
     */
    public List<RankItem> getTopRankList(int rankType, int start, int end) throws ExecutionException, InterruptedException {
        int neededCount = end; // 需要获取到第end名

        // 从每个分片获取前neededCount名
        List<CompletableFuture<List<ScoredEntry<Integer>>>> futures = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            String shardKey = rankKeyPrefix + rankType + ":" + i;
            CompletableFuture<List<ScoredEntry<Integer>>> future = CompletableFuture.supplyAsync(() -> {
                RScoredSortedSet<Integer> rankSet = redissonClient.getScoredSortedSet(shardKey);
                // 获取每个分片的前neededCount名（包含分数）
                Collection<ScoredEntry<Integer>> entries = rankSet.entryRangeReversed(0, neededCount - 1);
                return new ArrayList<>(entries);
            });
            futures.add(future);
        }

        // 合并所有分片结果
        List<ScoredEntry<Integer>> allEntries = new ArrayList<>();
        for (CompletableFuture<List<ScoredEntry<Integer>>> future : futures) {
            allEntries.addAll(future.get());
        }

        // 按分数降序排序
        allEntries.sort((a, b) -> Double.compare(b.getScore(), a.getScore()));

        // 转换为RankItem列表，并添加排名
        List<RankItem> result = new ArrayList<>();
        Long actualRank = 1L;
        for (int i = 0; i < allEntries.size(); i++) {
            ScoredEntry<Integer> entry = allEntries.get(i);
            double combineScore = entry.getScore();
            int playerId = entry.getValue();
            // 解析主分数
            long mainScore = parseMainScore(combineScore);
            actualRank = (long) (i + 1);
            result.add(new RankItem(playerId, mainScore, actualRank, combineScore));
        }

        // 返回指定范围的排名
        int fromIndex = Math.min(start - 1, result.size());
        int toIndex = Math.min(end, result.size());
        return result.subList(fromIndex, toIndex);
    }


    /**
     * 获取榜单前N名（优化版：使用最小堆避免内存爆炸）
     * 优化点：
     * 1. 使用PriorityQueue最小堆，仅保留Top N数据
     * 2. 内存占用从 200分片*10000 → 固定N条
     * 3. 避免全量排序，
     * @param rankType 榜单类型（1=战力榜，2=积分榜...）
     * @param start 起始排名（从1开始）
     * @param end 结束排名（从1开始）
     * @return 排名列表
     */
    @SneakyThrows
    public List<RankItem> getTopRankListOptimize(int rankType, int start, int end) {
        if (start < 1 || end < start) {
            throw new IllegalArgumentException("排名范围无效: start=" + start + ", end=" + end);
        }
        // 需要获取的实际数量（从第1名到第end名）
        int needCount = end;
        // ========== 使用最小堆聚合所有分片的Top N ==========
        // 定义最小堆：堆顶是分数最小的，方便淘汰
        PriorityQueue<ScoredEntry<Integer>> minHeap = new PriorityQueue<>(
                needCount + 1, // 容量稍大，避免频繁扩容
                Comparator.comparingDouble(ScoredEntry::getScore) // 按分数升序
        );

        // 并行查询所有分片，每个分片取needCount名
        List<CompletableFuture<Void>> futures = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            final int shardIndex = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    String shardKey = rankKeyPrefix + rankType + ":" + shardIndex;
                    RScoredSortedSet<Integer> rankSet = redissonClient.getScoredSortedSet(shardKey);

                    // 从每个分片获取前needCount名（降序：分数高在前）
                    Collection<ScoredEntry<Integer>> entries = rankSet.entryRangeReversed(0, needCount - 1);

                    // 同步加锁更新堆（多线程安全）
                    synchronized (minHeap) {
                        for (ScoredEntry<Integer> entry : entries) {
                            minHeap.offer(entry);
                            // 堆大小超过needCount，弹出最小值（淘汰机制）
                            if (minHeap.size() > needCount) {
                                minHeap.poll();
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("查询分片 {} 失败", shardIndex, e);
                }
            }, executor);
            futures.add(future);
        }

        // 等待所有分片查询完成
        try {
            // 1分钟超时
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            log.error("分片查询超时/异常，部分分片数据可能未合并", e);
            // 超时后仍继续处理已合并的数据，保证接口不返回空
        }

        // 堆转列表并降序排序（分数高在前）
        List<ScoredEntry<Integer>> topEntries = new ArrayList<>(minHeap);
        topEntries.sort((a, b) -> Double.compare(b.getScore(), a.getScore())); // 降序

        // ========== 第三步：转换为RankItem，处理同分排名逻辑 ==========
        List<RankItem> result = new ArrayList<>(topEntries.size());
        // 真实排名（同分排名相同）
        long actualRank = 1;
        // 当前索引（从0开始）
        int currentIndex = 0;

        for (ScoredEntry<Integer> entry : topEntries) {
            double combineScore = entry.getScore();
            Integer playerId = entry.getValue();
            // 解析主分数
            long mainScore = parseMainScore(combineScore);
            // 分数变化：排名 = 当前索引 + 1
            actualRank = currentIndex + 1;
            // 构建RankItem
            result.add(new RankItem(playerId, mainScore, actualRank, combineScore));
            currentIndex++;
        }

        // 截取指定范围 [start, end]
        // 注意：start/end 从1开始，列表索引从0开始
        int fromIndex = Math.max(0, start - 1);
        int toIndex = Math.min(end, result.size());
        if (fromIndex >= toIndex) {
            return new ArrayList<>(); // 范围无效，返回空列表
        }
        return result.subList(fromIndex, toIndex);
    }

    /**
     * 获取玩家排名区间（带分数）
     */
    public List<RankItem> getRankRange(int rankType, int start, int end) throws ExecutionException, InterruptedException {
        // 简单的实现：获取前end名，然后截取start-end部分
        // 更高效的实现需要优化，但考虑到分片，这是比较直接的方式,正常来讲都是取的top,
        List<RankItem> allTop = getTopRankList(rankType, 1, end);
        if (start > allTop.size()) {
            return new ArrayList<>();
        }
        return allTop.subList(start - 1, Math.min(end, allTop.size()));
    }

    /**
     * 获取玩家周围的排名（用于显示自己及前后几名）
     */
    public List<RankItem> getSurroundingRanks(Long playerId, int rankType, int before, int after)
            throws ExecutionException, InterruptedException {
        RankItem playerRank = getPlayerRankDetail(playerId, rankType);
        if (playerRank == null) {
            return new ArrayList<>();
        }
        long playerRankValue = playerRank.getRank();
        int start = (int) Math.max(1, playerRankValue - before);
        int end = (int) (playerRankValue + after);

        List<RankItem> range = getRankRange(rankType, start, end);
        // 标记当前玩家
        for (RankItem item : range) {
            if (item.getPlayerId().equals(playerId)) {
                // 可以在这里添加标记，或者调用方根据ID判断
                item.setIsCurrentUser(Boolean.TRUE);
            }
        }
        return range;
    }

    /**
     * 批量获取玩家分数和排名
     */
    public List<RankItem> batchGetRankDetails(List<Long> playerIds, int rankType) {
        List<RankItem> result = new ArrayList<>();

        // 按分片分组，减少Redis调用
        Map<String, List<Long>> shardPlayerMap = new HashMap<>();
        for (Long playerId : playerIds) {
            String shardKey = getShardKey(playerId, rankType);
            shardPlayerMap.computeIfAbsent(shardKey, k -> new ArrayList<>()).add(playerId);
        }

        // 分批查询
        for (Map.Entry<String, List<Long>> entry : shardPlayerMap.entrySet()) {
            String shardKey = entry.getKey();
            List<Long> shardPlayerIds = entry.getValue();
            RScoredSortedSet<Long> rankSet = redissonClient.getScoredSortedSet(shardKey);
            // 批量获取分数
            List<Double> scores = rankSet.getScore(shardPlayerIds);
            int index = 0;
            for (Long playerId : shardPlayerIds) {
                Double combineScore = scores.get(index++);
                if (combineScore != null) {
                    // todo 计算排名，这里是分片排名,要全局排名还得计算一下
                    long higherCount = rankSet.count(combineScore, true, Double.MAX_VALUE, false);
                    long rank = higherCount + 1;
                    long mainScore = parseMainScore(combineScore);
                    result.add(new RankItem(playerId, mainScore, rank, combineScore));
                }
            }
        }
        return result;
    }
}