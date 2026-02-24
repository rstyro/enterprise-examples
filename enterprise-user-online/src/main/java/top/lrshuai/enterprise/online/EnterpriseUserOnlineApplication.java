package top.lrshuai.enterprise.online;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 启动类：开启定时任务（用于在线总数预计算）
 */
@EnableScheduling
@SpringBootApplication
public class EnterpriseUserOnlineApplication {

    public static void main(String[] args) {
        SpringApplication.run(EnterpriseUserOnlineApplication.class, args);
    }

}
