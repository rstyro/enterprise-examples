## Redis的 Sorted Set （zset） 命令示例

```bash
# 添加数据，张三（100分）、李四（95分）、王五（98分）
ZADD player_scores 100 "zhangsan" 95 "lisi" 98 "wangwu"  90 "zhaoliu" 88 "sunqi" 80 "zhouba" 
# 添加数据
ZADD player_scores 60 "wujiu"


# 给整个 Key 设置过期时间（两种常用方式）
# 方式1：相对时间（单位：秒），示例：1小时后过期（3600秒）
EXPIRE player_scores 3600
# 方式2：绝对时间（单位：毫秒时间戳）
PEXPIREAT player_scores 1770266046000

# ====验证过期时间======
# 返回剩余过期时间（秒），-1=永不过期，-2=已过期
TTL player_scores
# 返回剩余过期时间（毫秒）
PTTL player_scores 



# 获取 王五 分数
ZSCORE player_scores "wangwu"
# 给王五增加10排序分数
ZINCRBY player_scores 10 "wangwu"


# 按排名查询，升序，前三名
ZRANGE player_scores 0 2 
# 按排名查询，升序，前三名及其积分WITHSCORES选项会同时返回积分
ZRANGE player_scores 0 2 WITHSCORES
# 按排名查询，升序,所有人
ZRANGE player_scores 0 -1 WITHSCORES

# 查询赵六排第几名，升序，索引从0开始
ZRANK player_scores "zhaoliu"
# 查询赵六排第几名，降序，索引从0开始
ZREVRANK player_scores "zhaoliu"
# 按排名查询，降序，前三名
ZREVRANGE player_scores 0 2 WITHSCORES

# ========== 按分数区间精准查询 ==========
# ZRANGEBYSCORE：按分数升序查85-100分的成员（含边界，返回成员+分数）
ZRANGEBYSCORE player_scores 85 100 WITHSCORES
# 查≤100分的成员（-inf表示负无穷）
ZRANGEBYSCORE player_scores -inf 100 WITHSCORES
# 查>90且≤100分的成员（( 表示开区间，不含90）
ZRANGEBYSCORE player_scores (90 100 WITHSCORES
# ZREVRANGEBYSCORE：按分数降序查90-110分的成员
ZREVRANGEBYSCORE player_scores 110 90 WITHSCORES


# ========== 弹出元素（移除+返回） ==========
# ZPOPMIN：弹出分数最小的1个成员（可指定数量，如ZPOPMIN key 3）
ZPOPMIN player_scores
# ZPOPMAX：弹出分数最大的1个成员
ZPOPMAX player_scores

# BZPOPMIN/BZPOPMAX：阻塞式弹出（无元素时阻塞，超时单位秒）
# 阻塞5秒，无元素返回nil
BZPOPMIN player_scores 5
# 阻塞10秒，适用于异步消费场景
BZPOPMAX player_scores 10


# 删除指定成员
ZREM player_scores "lisi"
# 删除多个成员
ZREM player_scores "lisi" "wangwu"
# 按积分区间删除，删除积分在0到50之间的所有成员
ZREMRANGEBYSCORE player_scores 0 50
# 按排名区间删除，删除正序排名中最低的三名玩家
ZREMRANGEBYRANK player_scores 0 2

# 查询积分区间人数
ZCOUNT player_scores 90 110
# 查询成员数量
ZCARD player_scores



# ========== 多集合聚合运算 ==========
# 先创建第二个集合（示例：另一场比赛的分数）
ZADD player_scores_2 95 "zhangsan" 92 "lisi" 98 "zhaoliu"

# ZUNIONSTORE：合并2个集合的并集，结果存入union_scores，分数求和
ZUNIONSTORE union_scores 2 player_scores player_scores_2 AGGREGATE SUM
# 查看并集结果
ZRANGE union_scores 0 -1 WITHSCORES  

# ZINTERSTORE：计算2个集合的交集，结果存入inter_scores，分数取最大值
ZINTERSTORE inter_scores 2 player_scores player_scores_2 AGGREGATE MAX
# 查看交集结果
ZRANGE inter_scores 0 -1 WITHSCORES  


# ========== 字典序查询（分数相同场景） ==========
# 创建分数全为0的集合（字典序查询要求分数一致）
ZADD dict_zset 0 "apple" 0 "banana" 0 "cherry" 0 "date"

# ZLEXCOUNT：统计字典序区间内的成员数（[b (d 表示b≤x<d）
# 输出：2（banana、cherry）
ZLEXCOUNT dict_zset [b (d
# ZRANGEBYLEX：按字典序查[b,c]区间的成员
ZRANGEBYLEX dict_zset [b [c  


# ========== 大数据量遍历 ==========
# ZSCAN：游标式遍历（适合百万级大集合），匹配以z开头的成员，每次返回5个
ZSCAN player_scores 0 MATCH "z*" COUNT 5

# ========== 批量操作（Redis 6.2+） ==========
# ZMSCORE：批量获取多个成员的分数，减少网络往返
ZMSCORE player_scores "zhangsan" "lisi" "zhaoliu"
```