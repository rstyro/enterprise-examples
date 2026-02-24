package top.lrshuai.enterprise.online.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 在线统计配置类
 */
@Data
@Component
@ConfigurationProperties(prefix = "online.counter")
public class OnlineProperties {
    // 分片数量
    private int shardCount = 16;
    // key 过期时间，单位：分钟
    private int expireMinute = 30;
    private long totalCountRefreshInterval = 1000;
    private CacheProperties l1Cache = new CacheProperties();
    private CacheProperties l2Cache = new CacheProperties();

    @Data
    public static class CacheProperties {
        private int maxSize = 1000000;
        private int expireSeconds = 5;
    }
}
