package top.lrshuai.enterprise.online.runner;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import top.lrshuai.enterprise.online.service.OnlineCounter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 模拟 1000万  用户在线压测
 * 启动项目自动跑，不想跑注释掉 @Component
 */
@Component
@RequiredArgsConstructor
public class OnlineUserSimulator implements CommandLineRunner {
    // 你想测试哪个实现，就注入哪个：
    // @Qualifier("redisTemplateOnlineCounter")
    private final OnlineCounter onlineCounter;

    // ================== 压测配置 ==================
    private static final long START_UID = 100000000000L;  // 起始ID
    private static final long USER_COUNT = 1000_0000L;   // 模拟多少人：1000万
    // private static final long USER_COUNT = 50_000_000L; // 5000万

    private static final int THREADS = 10;               // 并发线程
    private static final int BATCH_SIZE = 1000;          // 每批处理多少个

    private final AtomicLong success = new AtomicLong(0);

    @Override
    public void run(String... args) throws Exception {
        System.out.println("开始模拟 " + USER_COUNT / 10000 + "万 用户在线...");
        long start = System.currentTimeMillis();

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);

        // 分段提交任务（避免一次性提交过多）
        for (long i = 0; i < USER_COUNT; i += BATCH_SIZE) {
            long batchStart = i;
            pool.submit(() -> {
                for (int j = 0; j < BATCH_SIZE; j++) {
                    long uid = START_UID + batchStart + j;
                    try {
                        onlineCounter.userOnline(uid);
                        success.incrementAndGet();
                        // 每处理1000个打印一次进度
                        if (success.get() % 10000 == 0) {
                            System.out.println("已模拟 " + success.get() / 10000 + "万 用户在线");
                        }
                    } catch (Exception e) {
                        System.err.println("用户" + uid + "上线失败：" + e.getMessage());
                    }
                }
            });
        }

        pool.shutdown();
        boolean finished = pool.awaitTermination(30, TimeUnit.MINUTES); // 延长超时时间到30分钟
        if (!finished) {
            pool.shutdownNow();
            System.err.println("任务超时，强制终止");
        }

        // 输出结果
        long cost = System.currentTimeMillis() - start;
        System.out.println("===== 模拟完成 =====");
        System.out.println("总耗时：" + cost / 1000 + "秒");
        System.out.println("成功上线用户数：" + success.get());
        // 等定时任务最新刷新一次
        Thread.sleep(3000);
        System.out.println("Redis统计在线总数：" + onlineCounter.getOnlineTotal());
    }
}
