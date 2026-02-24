package top.lrshuai.enterprise.online.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;
import top.lrshuai.enterprise.online.config.OnlineProperties;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * 在线统计抽象基类：抽离所有重复逻辑
 */
@Slf4j
public abstract class AbstractOnlineCounter implements OnlineCounter{

    // 通用常量
    protected static final String BITMAP_KEY_PREFIX = "online_bitmap_";
    protected static final String TOTAL_COUNT_KEY = "online_total_count";

    // 配置注入
    @Resource
    protected OnlineProperties onlineProperties;

    // 32位Redis最大安全偏移量（2^32 - 1）=4294967295，避免offset越界
    protected static final long MAX_SAFE_OFFSET = (1L << 32) - 1;

    // 多级缓存:L1（热点缓存，最快）、L2（次热缓存，兜底）
    protected Cache<Long, Boolean> l1Cache;
    protected Cache<Long, Boolean> l2Cache;

    // 本地缓存的在线总数
    protected volatile long cachedTotalCount;

    /**
     * 初始化：缓存创建
     */
    @PostConstruct
    public void init() {
        // 初始化L1缓存
        l1Cache = CacheBuilder.newBuilder()
                .maximumSize(onlineProperties.getL1Cache().getMaxSize())
                .expireAfterWrite(onlineProperties.getL1Cache().getExpireSeconds(), TimeUnit.SECONDS)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .build();

        // 初始化L2缓存
        l2Cache = CacheBuilder.newBuilder()
                .maximumSize(onlineProperties.getL2Cache().getMaxSize())
                .expireAfterWrite(onlineProperties.getL2Cache().getExpireSeconds(), TimeUnit.SECONDS)
                .build();

        log.info("在线统计基类初始化完成，分片数：{}，L1缓存最大条数：{}",
                onlineProperties.getShardCount(), onlineProperties.getL1Cache().getMaxSize());
    }

    /**
     * 分片索引计算
     */
    protected int getShardIndex(Long userId) {
        Assert.notNull(userId, "用户ID不能为空");
        return Math.abs(userId.hashCode() % onlineProperties.getShardCount());
    }

    /**
     * 获取分片Bitmap Key
     */
    protected String getShardBitmapKey(int shardIndex) {
        return BITMAP_KEY_PREFIX + shardIndex;
    }

    /**
     * 参数校验
     */
    protected void validateUserId(Long userId) {
        Assert.notNull(userId, "用户ID不能为空");
        Assert.isTrue(userId >= 0, "用户ID不能为负数");
    }

    /**
     * 更新本地缓存
     */
    protected void updateLocalCache(Long userId, boolean isOnline) {
        l1Cache.put(userId, isOnline);
        l2Cache.put(userId, isOnline);
    }

    /**
     * 将超大用户ID映射,Redis Bitmap支持的offset范围（0 ~ 2^32-1）
     * 解决Snowflake等超大ID的Bitmap偏移越界问题
     */
    protected long mapUserIdToOffset(Long userId) {
        // 最大安全偏移量
//        final long MAX_SAFE_OFFSET = (1L << 32) - 1; // 4294967295
        // 取绝对值后取模，保证永远在安全范围
        return Math.abs(Long.hashCode(userId)) % MAX_SAFE_OFFSET;
    }

    /**
     * 通用的总数汇总逻辑（子类只需实现分片计数）
     */
    @Override
    public void refreshTotalCount() {
        LongAdder total = new LongAdder();
        try {
            // 遍历所有分片，调用子类实现的分片计数方法
            for (int i = 0; i < onlineProperties.getShardCount(); i++) {
                long shardCount = calculateShardCount(i);
                total.add(shardCount);
            }

            long totalCount = total.sum();
            // 调用子类实现的总数缓存方法
            cacheTotalCount(totalCount);
            // 更新本地缓存
            this.cachedTotalCount = totalCount;

            log.debug("在线总数刷新完成，当前在线人数：{}", totalCount);
        } catch (Exception e) {
            log.error("刷新在线总数失败", e);
        }
    }

    /**
     * 通用的在线总数查询逻辑（子类只需实现Redis总数查询）
     */
    @Override
    public long getOnlineTotal() {
        // 1. 优先查本地缓存
        if (this.cachedTotalCount > 0) {
            return this.cachedTotalCount;
        }
        // 2. 调用子类实现的Redis查询方法
        try {
            return queryTotalCountFromRedis();
        } catch (Exception e) {
            log.error("查询在线总数失败", e);
            return 0;
        }
    }

    /**
     * 通用的isOnline逻辑（子类只需实现Redis Bitmap查询）
     */
    @Override
    public boolean isOnline(Long userId) {
        try {
            validateUserId(userId);

            // 1. 查L1缓存
            Boolean l1Result = l1Cache.getIfPresent(userId);
            if (l1Result != null) {
                return l1Result;
            }

            // 2. 查L2缓存
            Boolean l2Result = l2Cache.getIfPresent(userId);
            if (l2Result != null) {
                l1Cache.put(userId, l2Result);
                return l2Result;
            }

            // 3. 调用子类实现的Redis查询方法
            int shardIndex = getShardIndex(userId);
            boolean isOnline = queryBitmapFromRedis(shardIndex, userId);

            // 4. 回写本地缓存
            updateLocalCache(userId, isOnline);
            return isOnline;
        } catch (Exception e) {
            log.error("查询用户在线状态失败，userId：{}", userId, e);
            return false;
        }
    }

    /**
     * 销毁资源
     */
    @PreDestroy
    public void destroy() {
        l1Cache.invalidateAll();
        l2Cache.invalidateAll();
        log.info("在线统计服务已销毁，缓存已清理");
    }

    // ========== 抽象方法：子类需实现的差异化逻辑 ==========
    /**
     * 子类实现：设置Bitmap为1（用户上线）
     */
    protected abstract void setBitmapOnline(int shardIndex, Long userId);

    /**
     * 子类实现：设置Bitmap为0（用户下线）
     */
    protected abstract void setBitmapOffline(int shardIndex, Long userId);

    /**
     * 子类实现：查询Redis Bitmap状态（用户是否在线）
     */
    protected abstract boolean queryBitmapFromRedis(int shardIndex, Long userId);

    /**
     * 子类实现：计算单个分片的在线数
     */
    protected abstract long calculateShardCount(int shardIndex);

    /**
     * 子类实现：缓存总数到Redis
     */
    protected abstract void cacheTotalCount(long totalCount);

    /**
     * 子类实现：从Redis查询总数
     */
    protected abstract long queryTotalCountFromRedis();
}
