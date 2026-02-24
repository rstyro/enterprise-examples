package top.lrshuai.enterprise.online.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Redis实现的在线统计服务
 * 核心：Redis Bitmap + 分片 + 多级缓存 + 预计算总数
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisTemplateOnlineCounter extends AbstractOnlineCounter {

    private final RedisTemplate<String, Object> redisTemplate;

    @Override
    public void userOnline(Long userId) {
        try {
            validateUserId(userId);
            int shardIndex = getShardIndex(userId);
            // 调用子类实现的Bitmap设置方法
            setBitmapOnline(shardIndex, userId);
            // 更新本地缓存
            updateLocalCache(userId, true);
            log.debug("RedisTemplate：用户{}上线，分片索引：{}", userId, shardIndex);
        } catch (Exception e) {
            log.error("RedisTemplate：用户上线失败，userId：{}", userId, e);
        }
    }

    @Override
    public void userOffline(Long userId) {
        try {
            validateUserId(userId);
            int shardIndex = getShardIndex(userId);
            // 调用子类实现的Bitmap设置方法
            setBitmapOffline(shardIndex, userId);
            // 更新本地缓存
            updateLocalCache(userId, false);
            log.debug("RedisTemplate：用户{}下线，分片索引：{}", userId, shardIndex);
        } catch (Exception e) {
            log.error("RedisTemplate：用户下线失败，userId：{}", userId, e);
        }
    }

    @Override
    protected void setBitmapOnline(int shardIndex, Long userId) {
        String bitmapKey = getShardBitmapKey(shardIndex);
        long offset = mapUserIdToOffset(userId);
        // RedisTemplate设置Bitmap为1
        redisTemplate.opsForValue().setBit(bitmapKey, offset, true);
        // 设置过期时间,兜底
        redisTemplate.expire(bitmapKey, onlineProperties.getExpireMinute(), TimeUnit.MINUTES);
    }

    @Override
    protected void setBitmapOffline(int shardIndex, Long userId) {
        String bitmapKey = getShardBitmapKey(shardIndex);
        long offset = mapUserIdToOffset(userId);
        // RedisTemplate设置Bitmap为0
        redisTemplate.opsForValue().setBit(bitmapKey, offset, false);
    }

    @Override
    protected boolean queryBitmapFromRedis(int shardIndex, Long userId) {
        String bitmapKey = getShardBitmapKey(shardIndex);
        long offset = mapUserIdToOffset(userId);
        return Boolean.TRUE.equals(redisTemplate.opsForValue().getBit(bitmapKey, offset));
    }

    @Override
    protected long calculateShardCount(int shardIndex) {
        String bitmapKey = getShardBitmapKey(shardIndex);
        // 调用BITCOUNT命令
        return (Long) redisTemplate.execute((RedisCallback<Object>) connection ->
                connection.bitCount(bitmapKey.getBytes())
        );
    }

    @Override
    protected void cacheTotalCount(long totalCount) {
        redisTemplate.opsForValue().set(TOTAL_COUNT_KEY, totalCount, 5, TimeUnit.MINUTES);
    }

    @Override
    protected long queryTotalCountFromRedis() {
        Long totalCount = (Long) redisTemplate.opsForValue().get(TOTAL_COUNT_KEY);
        return totalCount == null ? 0 : totalCount;
    }

    /**
     * 定时任务：刷新总数（RedisTemplate版）
     */
    @Scheduled(fixedRateString = "${online.counter.total-count-refresh-interval}")
    @Override
    public void refreshTotalCount() {
        super.refreshTotalCount();
    }

}
