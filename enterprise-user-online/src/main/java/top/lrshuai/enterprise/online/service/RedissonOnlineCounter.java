package top.lrshuai.enterprise.online.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBitSet;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Redisson实现的在线统计服务
 */
@Slf4j
@Service
@Primary
@RequiredArgsConstructor
public class RedissonOnlineCounter extends AbstractOnlineCounter{

    private final RedissonClient redissonClient;

    @Override
    public void userOnline(Long userId) {
        try {
            validateUserId(userId);
            int shardIndex = getShardIndex(userId);
            setBitmapOnline(shardIndex, userId);
            updateLocalCache(userId, true);
            log.debug("Redisson：用户{}上线，分片索引：{}", userId, shardIndex);
        } catch (Exception e) {
            log.error("Redisson：用户上线失败，userId：{}", userId, e);
        }
    }

    @Override
    public void userOffline(Long userId) {
        try {
            validateUserId(userId);
            int shardIndex = getShardIndex(userId);
            setBitmapOffline(shardIndex, userId);
            updateLocalCache(userId, false);
            log.debug("Redisson：用户{}下线，分片索引：{}", userId, shardIndex);
        } catch (Exception e) {
            log.error("Redisson：用户下线失败，userId：{}", userId, e);
        }
    }

    @Override
    protected void setBitmapOnline(int shardIndex, Long userId) {
        RBitSet bitSet = redissonClient.getBitSet(getShardBitmapKey(shardIndex));
        // Redisson设置Bitmap为1
        long offset = mapUserIdToOffset(userId);
        bitSet.set(offset);
        // 设置过期时间
        bitSet.expire(onlineProperties.getExpireMinute(), TimeUnit.MINUTES);
    }

    @Override
    protected void setBitmapOffline(int shardIndex, Long userId) {
        RBitSet bitSet = redissonClient.getBitSet(getShardBitmapKey(shardIndex));
        // Redisson设置Bitmap为0
        bitSet.clear(mapUserIdToOffset(userId));
    }

    @Override
    protected boolean queryBitmapFromRedis(int shardIndex, Long userId) {
        RBitSet bitSet = redissonClient.getBitSet(getShardBitmapKey(shardIndex));
        return bitSet.get(mapUserIdToOffset(userId));
    }

    @Override
    protected long calculateShardCount(int shardIndex) {
        RBitSet bitSet = redissonClient.getBitSet(getShardBitmapKey(shardIndex));
        // Redisson统计Bitmap中1的数量
        return bitSet.cardinality();
    }

    @Override
    protected void cacheTotalCount(long totalCount) {
        redissonClient.getBucket(TOTAL_COUNT_KEY).set(totalCount, 5, TimeUnit.MINUTES);
    }

    @Override
    protected long queryTotalCountFromRedis() {
        Object number = redissonClient.getBucket(TOTAL_COUNT_KEY).get();
        return number == null ? 0 : Long.parseLong(number.toString());
    }
}
