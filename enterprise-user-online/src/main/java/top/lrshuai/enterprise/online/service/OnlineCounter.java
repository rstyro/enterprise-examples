package top.lrshuai.enterprise.online.service;

/**
 * 在线统计核心接口
 */
public interface OnlineCounter {
    /**
     * 用户上线/心跳续期
     */
    void userOnline(Long userId);

    /**
     * 用户下线
     */
    void userOffline(Long userId);

    /**
     * 判断用户是否在线
     */
    boolean isOnline(Long userId);

    /**
     * 获取在线总人数
     */
    long getOnlineTotal();

    /**
     * 刷新在线总数（供定时任务调用）
     */
    void refreshTotalCount();
}
