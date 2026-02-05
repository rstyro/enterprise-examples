package top.lrshuai.enterprise.ranks.vo;

import lombok.Data;

/**
 * 排名结果对象
 */
@Data
public class RankItem {

    private Object playerId;     // 玩家ID
    private Long mainScore;    // 主分数（实际游戏分数）
    private Long rank;         // 排名（1-based）
    private Double combineScore; // 组合分数（内部使用）
    private Boolean isCurrentUser;

    public RankItem(Object playerId, Long mainScore, Long rank, Double combineScore) {
        this.playerId = playerId;
        this.mainScore = mainScore;
        this.rank = rank;
        this.combineScore = combineScore;
    }

    public static void main(String[] args) {
        System.out.println(Integer.MAX_VALUE-1);
    }
}
