package top.lrshuai.enterprise.ranks.vo;

public class Demo {
    // 主分数占用高30位，时间戳占用低34位
    private static final int TIMESTAMP_BITS = 34;   // 最多2^34 = 17,179,869,184
    private static final long TIMESTAMP_MASK = (1L << TIMESTAMP_BITS) - 1;

    /**
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
    public static long parseMainScore(long combinedScore) {
        // 右移34位获取主分数
        return combinedScore >>> TIMESTAMP_BITS;
    }

    public static void main(String[] args) {
        System.out.println("=== 开始验证分数计算准确性 ===");
        long time = System.currentTimeMillis();
        // 测试极端情况
        long[][] testCases = {
                {10L, time},
                {1L, time},
                {100L, time},
                {1000000L, time},
                {500000000L, time},
                {999999999L, time},
                {999999999L, time+1},
                {1000000000L, time}
        };

        for (long[] testCase : testCases) {
            long mainScore = testCase[0];
            long updateTime = testCase[1];

            double combined = combineScore(mainScore, updateTime);
            long parsed = parseMainScore((long) combined);
            System.out.println("maiSore=%s,updateTime=%s,combined=%s,parse=%s".formatted(mainScore, updateTime, combined, parsed));
            if (mainScore != parsed) {
                String error = String.format("准确性验证失败: 输入=%d, 输出=%d, 组合分数=%f",
                        mainScore, parsed, combined);
                throw new IllegalStateException(error);
            }
            System.out.printf("验证通过: 主分数=%d, 解析分数=%d\n", mainScore, parsed);
        }
        System.out.println("=== 所有验证通过 ===");

        System.out.println(combineScore(9998, time)<combineScore(9999, time));
        System.out.println(combineScore(999999999, time)==combineScore(999999999, time+1));
        System.out.println(combineScore(999999999, time)>combineScore(999999999, time+1));
        System.out.println(combineScore(999999999, time)<combineScore(999999999, time+1));
        System.out.println(System.currentTimeMillis());
        System.out.println(System.currentTimeMillis()& TIMESTAMP_MASK);
        System.out.println(1770195175 & TIMESTAMP_MASK);
        System.out.println(1870295175 & TIMESTAMP_MASK);
    }

}
