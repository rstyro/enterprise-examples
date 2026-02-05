package top.lrshuai.enterprise.ranks.service;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import top.lrshuai.enterprise.ranks.vo.RankItem;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 玩家排名核心服务
 */
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
                // 对于当前玩家所在的分片，排除等于自己分数的情况
                if (currentShardKey.equals(shardKey)) {
                    return shardSet.count(combineScore, false, Double.MAX_VALUE, true);
                }
                // 对于其他分片，统计所有分数大于当前分数的玩家
                else {
                    return shardSet.count(combineScore, false, Double.MAX_VALUE, true);
                }
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
                double minScore = mainScore * 10000000000L;
                double maxScore = (mainScore + 1) * 10000000000L - 1;

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
        int currentRank = 0;
        Double lastScore = null;
        Long actualRank = 1L;

        for (int i = 0; i < allEntries.size(); i++) {
            ScoredEntry<Integer> entry = allEntries.get(i);
            double combineScore = entry.getScore();
            int playerId = entry.getValue();

            // 处理相同分数的排名
            if (lastScore == null || Double.compare(combineScore, lastScore) != 0) {
                actualRank = (long) (i + 1);
                currentRank = i;
                lastScore = combineScore;
            } else {
                // 同分数，排名相同
                actualRank = (long) (currentRank + 1);
            }
            // 解析主分数
            long mainScore = parseMainScore(combineScore);
            result.add(new RankItem(playerId, mainScore, actualRank, combineScore));
        }

        // 返回指定范围的排名
        int fromIndex = Math.min(start - 1, result.size());
        int toIndex = Math.min(end, result.size());
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