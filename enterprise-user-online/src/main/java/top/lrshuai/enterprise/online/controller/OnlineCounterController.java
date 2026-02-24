package top.lrshuai.enterprise.online.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import top.lrshuai.enterprise.online.service.RedisTemplateOnlineCounter;
import top.lrshuai.enterprise.online.service.RedissonOnlineCounter;

/**
 * 用户在线统计服务接口测试
 */
@RestController
@RequestMapping("/online")
@RequiredArgsConstructor
public class OnlineCounterController {

    // 注入RedisTemplate实现
    private final RedisTemplateOnlineCounter redisTemplateCounter;

    // 注入Redisson实现
    private final RedissonOnlineCounter redissonCounter;

    /**
     * 使用RedisTemplate实现：用户上线
     */
    @PostMapping("/redis/{userId}")
    public String redisOnline(@PathVariable Long userId) {
        redisTemplateCounter.userOnline(userId);
        return "RedisTemplate：用户" + userId + "已上线";
    }

    /**
     * 使用Redisson实现：用户上线
     */
    @PostMapping("/redisson/{userId}")
    public String redissonOnline(@PathVariable Long userId) {
        redissonCounter.userOnline(userId);
        return "Redisson：用户" + userId + "已上线";
    }

    /**
     * 使用RedisTemplate实现：查询是否在线
     */
    @GetMapping("/redis/{userId}")
    public Boolean redisIsOnline(@PathVariable Long userId) {
        return redisTemplateCounter.isOnline(userId);
    }

    /**
     * 使用Redisson实现：查询是否在线
     */
    @GetMapping("/redisson/{userId}")
    public Boolean redissonIsOnline(@PathVariable Long userId) {
        return redissonCounter.isOnline(userId);
    }

    /**
     * 使用RedisTemplate实现：查询总人数
     */
    @GetMapping("/redis/total")
    public Long redisTotal() {
        return redisTemplateCounter.getOnlineTotal();
    }

    /**
     * 使用Redisson实现：查询总人数
     */
    @GetMapping("/redisson/total")
    public Long redissonTotal() {
        return redissonCounter.getOnlineTotal();
    }
}
