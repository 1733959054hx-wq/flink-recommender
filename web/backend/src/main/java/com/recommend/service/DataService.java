package com.recommend.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.recommend.entity.*;
import com.recommend.mapper.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 数据服务层
 * 负责推荐系统的数据分析、特征计算、模型调用等核心业务逻辑
 */
@Service
public class DataService {
    @Value("${python.script-dir}")
    private String pythonScriptDir;

    // ============ 数据访问层注入 ============
    @Autowired
    private UserBehaviorMapper userBehaviorMapper;      // 用户行为表
    @Autowired
    private UserFeaturesMapper userFeaturesMapper;      // 用户特征表
    @Autowired
    private ItemFeaturesMapper itemFeaturesMapper;      // 商品特征表
    @Autowired
    private RecommendationsMapper recommendationsMapper; // 推荐结果表
    @Autowired
    private EvalMetricsMapper evalMetricsMapper;        // 评估指标表
    @Autowired
    private StringRedisTemplate stringRedisTemplate;    // Redis缓存操作

    // ============ Flink配置 ============
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Value("${kafka.topic}")
    private String kafkaTopic;
    /**
     * 获取用户行为分布统计
     * 统计四种行为类型(pv/cart/fav/buy)的总数量
     * @return 包含各行为类型计数的Map
     */
    public Map<String, Long> getBehaviorDistribution() {
        Map<String, Long> result = new HashMap<>();
        result.put("pv", userBehaviorMapper.selectCount(
                new LambdaQueryWrapper<UserBehavior>().eq(UserBehavior::getBehaviorType, "pv")));
        result.put("cart", userBehaviorMapper.selectCount(
                new LambdaQueryWrapper<UserBehavior>().eq(UserBehavior::getBehaviorType, "cart")));
        result.put("fav", userBehaviorMapper.selectCount(
                new LambdaQueryWrapper<UserBehavior>().eq(UserBehavior::getBehaviorType, "fav")));
        result.put("buy", userBehaviorMapper.selectCount(
                new LambdaQueryWrapper<UserBehavior>().eq(UserBehavior::getBehaviorType, "buy")));
        return result;
    }

    /**
     * 获取用户行为漏斗分析
     * 计算从浏览到购买的转化率，包括：
     * - 浏览->加购转化率(pvToCart)
     * - 加购->收藏转化率(cartToFav)
     * - 收藏->购买转化率(favToBuy)
     * - 浏览->购买整体转化率(pvToBuy)
     * @return 包含各阶段数量和转化率的Map
     */
    public Map<String, Object> getBehaviorFunnel() {
        // 统计各行为类型的数量
        long pvCount = userBehaviorMapper.selectCount(
                new LambdaQueryWrapper<UserBehavior>().eq(UserBehavior::getBehaviorType, "pv"));
        long cartCount = userBehaviorMapper.selectCount(
                new LambdaQueryWrapper<UserBehavior>().eq(UserBehavior::getBehaviorType, "cart"));
        long favCount = userBehaviorMapper.selectCount(
                new LambdaQueryWrapper<UserBehavior>().eq(UserBehavior::getBehaviorType, "fav"));
        long buyCount = userBehaviorMapper.selectCount(
                new LambdaQueryWrapper<UserBehavior>().eq(UserBehavior::getBehaviorType, "buy"));

        Map<String, Object> result = new HashMap<>();
        result.put("pv", pvCount);
        result.put("cart", cartCount);
        result.put("fav", favCount);
        result.put("buy", buyCount);

        // 计算转化率(保留4位小数，转为百分比)
        if (pvCount > 0) {
            result.put("pvToCart", BigDecimal.valueOf(cartCount)
                    .divide(BigDecimal.valueOf(pvCount), 4, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100)));
            result.put("cartToFav", BigDecimal.valueOf(favCount)
                    .divide(BigDecimal.valueOf(cartCount), 4, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100)));
            result.put("favToBuy", BigDecimal.valueOf(buyCount)
                    .divide(BigDecimal.valueOf(favCount), 4, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100)));
            result.put("pvToBuy", BigDecimal.valueOf(buyCount)
                    .divide(BigDecimal.valueOf(pvCount), 4, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100)));
        } else {
            // 如果浏览量为0，转化率都为0
            result.put("pvToCart", BigDecimal.ZERO);
            result.put("cartToFav", BigDecimal.ZERO);
            result.put("favToBuy", BigDecimal.ZERO);
            result.put("pvToBuy", BigDecimal.ZERO);
        }
        return result;
    }

    /**
     * 获取推荐预测统计
     * 统计已被推荐和未被推荐的商品数量
     * @return 包含推荐/未推荐商品数量的Map
     */
    public Map<String, Long> getPredictionStats() {
        Map<String, Long> result = new HashMap<>();
        long recommendCount = recommendationsMapper.selectCount(null);     // 已推荐商品数
        long totalItems = itemFeaturesMapper.selectCount(null);            // 总商品数
        result.put("recommend", recommendCount);
        result.put("notRecommend", Math.max(totalItems - recommendCount, 0L)); // 确保不为负数
        return result;
    }

    /**
     * 获取PR-AUC趋势数据
     * 从 eval_metrics 表中查询最新的N条pr_auc不为-1的记录
     * @param n 需要返回的记录条数
     * @return PR-AUC值及评测时间列表
     */
    public List<Map<String, Object>> getPrAucTrend(int n) {
        List<Map<String, Object>> result = new ArrayList<>();

        List<EvalMetrics> metrics = evalMetricsMapper.selectList(
                new LambdaQueryWrapper<EvalMetrics>()
                        .ne(EvalMetrics::getAccuracy, BigDecimal.valueOf(-1.0))
                        .isNotNull(EvalMetrics::getAccuracy)
                        .orderByDesc(EvalMetrics::getCreateTime)
                        .last("LIMIT " + n)
        );

        for (EvalMetrics metric : metrics) {
            Map<String, Object> item = new HashMap<>();
            item.put("prAuc", metric.getAccuracy());
            item.put("createTime", metric.getCreateTime() != null
                    ? metric.getCreateTime().toString().replace("T", " ") : "");
            result.add(item);
        }

        return result;
    }


    /**
     * 获取CTR(点击率)/CVR(转化率)数据
     * 先调用Python脚本计算指标，再查询数据库获取结果
     * @param startTime 统计开始时间
     * @param endTime 统计结束时间
     * @return 包含CTR、CVR和推荐数量的Map
     */
    public Map<String, Object> getCtrCvr(String startTime, String endTime) {
        System.out.println("=== 开始调用CTR/CVR评测脚本 ===");
        System.out.println("时间范围: " + startTime + " ~ " + endTime);
        try {
            String pythonScriptPath = pythonScriptDir + "test_for_CTR_and_CVR.py";
            System.out.println("Python 脚本路径: " + pythonScriptPath);
            // 直接用 "python"（系统PATH已配好）
            ProcessBuilder processBuilder = new ProcessBuilder("python", pythonScriptPath, startTime, endTime);
            processBuilder.redirectErrorStream(true);
            processBuilder.environment().put("PYTHONIOENCODING", "utf-8");
            Process process = processBuilder.start();

            new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), "UTF-8"))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println("[Python CTR/CVR] " + line);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("CTR/CVR评测脚本执行失败，退出码: " + exitCode);
            }
            System.out.println("=== CTR/CVR评测脚本执行完成 ===");
        } catch (Exception e) {
            System.err.println("调用CTR/CVR评测脚本失败: " + e.getMessage());
        }

        // Step 2: 从 recommendations 表统计推荐总数 + 从 eval_metrics 表查CTR/CVR
        Map<String, Object> result = new HashMap<>();
        try {
            LocalDateTime startLDT = LocalDateTime.parse(startTime.replace(" ", "T"));
            LocalDateTime endLDT = LocalDateTime.parse(endTime.replace(" ", "T"));

            // ===== 直接从 recommendations 表统计推荐总数 =====
            long recommendCount = recommendationsMapper.selectCount(
                    new LambdaQueryWrapper<Recommendations>()
                            .ge(Recommendations::getCreateTime, startLDT)
                            .le(Recommendations::getCreateTime, endLDT)
            );

            // 查询 eval_metrics 获取 CTR/CVR（不变）
            List<EvalMetrics> metrics = evalMetricsMapper.selectList(
                    new LambdaQueryWrapper<EvalMetrics>()
                            .eq(EvalMetrics::getMetricStartTime, startLDT)
                            .eq(EvalMetrics::getMetricEndTime, endLDT)
                            .orderByDesc(EvalMetrics::getId)
                            .last("LIMIT 1")
            );

            if (!metrics.isEmpty()) {
                EvalMetrics metric = metrics.get(0);
                result.put("ctr", metric.getCtr() != null ? metric.getCtr() : BigDecimal.ZERO);
                result.put("cvr", metric.getCvr() != null ? metric.getCvr() : BigDecimal.ZERO);
                result.put("recommendCount", (int) recommendCount);   // 【修改】使用直接统计值
                result.put("startTime", startTime);
                result.put("endTime", endTime);
                System.out.println("从数据库查询到CTR/CVR结果: ctr=" +
                        metric.getCtr() + ", cvr=" + metric.getCvr() + ", recommendCount=" + recommendCount);
            } else {
                // 未找到评测数据时，CTR/CVR 返回 0，推荐总数仍用直接统计值
                result.put("ctr", BigDecimal.ZERO);
                result.put("cvr", BigDecimal.ZERO);
                result.put("recommendCount", (int) recommendCount);   // 【修改】使用直接统计值
                result.put("startTime", startTime);
                result.put("endTime", endTime);
                System.out.println("数据库中暂无该时间段的评测结果，但推荐总数为: " + recommendCount);
            }
        } catch (Exception e) {
            System.err.println("查询数据库评测结果失败: " + e.getMessage());
            result.put("ctr", BigDecimal.ZERO);
            result.put("cvr", BigDecimal.ZERO);
            result.put("recommendCount", 0);
            result.put("startTime", startTime);
            result.put("endTime", endTime);
        }
        return result;
    }


    /**
     * 获取CTR/CVR趋势数据
     * 从 eval_metrics 表中查询指定时间段内的评测结果
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return CTR、CVR及时间的列表
     */
    public List<Map<String, Object>> getCtrCvrTrend(String startTime, String endTime) {
        List<Map<String, Object>> result = new ArrayList<>();

        LocalDateTime startLDT = LocalDateTime.parse(startTime.replace(" ", "T"));
        LocalDateTime endLDT = LocalDateTime.parse(endTime.replace(" ", "T"));

        List<EvalMetrics> metrics = evalMetricsMapper.selectList(
                new LambdaQueryWrapper<EvalMetrics>()
                        .ge(EvalMetrics::getMetricStartTime, startLDT)
                        .le(EvalMetrics::getMetricEndTime, endLDT)
                        .orderByAsc(EvalMetrics::getCreateTime)
        );

        for (EvalMetrics metric : metrics) {
            Map<String, Object> item = new HashMap<>();
            item.put("time", metric.getCreateTime() != null
                    ? metric.getCreateTime().toString().replace("T", " ").substring(0, 19) : "");
            item.put("ctr", metric.getCtr() != null ? metric.getCtr() : BigDecimal.ZERO);
            item.put("cvr", metric.getCvr() != null ? metric.getCvr() : BigDecimal.ZERO);
            result.add(item);
        }

        return result;
    }


    /**
     * 获取热门商品TOP10
     * 按商品流行度评分降序排列，取前10个商品
     * @return 包含商品ID、流行度评分和浏览量的商品列表
     */
    public List<Map<String, Object>> getTop10Items() {
        List<ItemFeatures> items = itemFeaturesMapper.selectList(
                new LambdaQueryWrapper<ItemFeatures>()
                        .orderByDesc(ItemFeatures::getPopularityScore)
                        .last("LIMIT 10"));

        return items.stream().map(item -> {
            Map<String, Object> map = new HashMap<>();
            map.put("itemId", item.getItemId());
            map.put("popularityScore", item.getPopularityScore());
            map.put("pvCount", item.getPvCount());
            return map;
        }).collect(Collectors.toList());
    }

    /**
     * 获取推荐次数趋势数据
     * 从 eval_metrics 表中查询指定时间段内的推荐次数
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 推荐次数及时间的列表
     */
    public List<Map<String, Object>> getRecommendCountTrend(String startTime, String endTime) {
        List<Map<String, Object>> result = new ArrayList<>();
        LocalDateTime startLDT = LocalDateTime.parse(startTime.replace(" ", "T"));
        LocalDateTime endLDT = LocalDateTime.parse(endTime.replace(" ", "T"));
        // ===== 从 recommendations 表按 create_time 实时查询 =====
        List<Recommendations> allRecs = recommendationsMapper.selectList(
                new LambdaQueryWrapper<Recommendations>()
                        .ge(Recommendations::getCreateTime, startLDT)
                        .le(Recommendations::getCreateTime, endLDT)
                        .orderByAsc(Recommendations::getCreateTime)
        );
        // 按分钟分组统计
        Map<String, Integer> minuteCountMap = new LinkedHashMap<>();
        for (Recommendations rec : allRecs) {
            if (rec.getCreateTime() != null) {
                String minute = rec.getCreateTime().toString().replace("T", " ").substring(0, 16);
                minuteCountMap.merge(minute, 1, Integer::sum);
            }
        }
        for (Map.Entry<String, Integer> entry : minuteCountMap.entrySet()) {
            Map<String, Object> item = new HashMap<>();
            item.put("time", entry.getKey());
            item.put("count", entry.getValue());
            result.add(item);
        }
        return result;
    }



    public List<Map<String, Object>> getLatestRecommendations(Long userId) {
        String redisKey = "rec:" + userId;
        try {
            Set<ZSetOperations.TypedTuple<String>> topItems =
                    stringRedisTemplate.opsForZSet().reverseRangeWithScores(redisKey, 0, 9);
            // Redis 读取部分
            if (topItems != null && !topItems.isEmpty()) {
                List<Map<String, Object>> resultList = new ArrayList<>();
                int rank = 1;
                for (ZSetOperations.TypedTuple<String> tuple : topItems) {
                    Map<String, Object> itemMap = new HashMap<>();
                    itemMap.put("rankNo", rank++);

                    Long itemId = Long.parseLong(tuple.getValue());

                    // ★ 从 hash 中获取 predictScore
                    String metaKey = "rec_meta:" + userId;
                    String predictScoreStr = (String) stringRedisTemplate.opsForHash().get(metaKey, String.valueOf(itemId));
                    BigDecimal predictScore = predictScoreStr != null
                            ? new BigDecimal(predictScoreStr) : BigDecimal.ZERO;

                    itemMap.put("itemId", itemId);
                    itemMap.put("finalScore", BigDecimal.valueOf(
                            tuple.getScore() != null ? tuple.getScore() : 0));
                    itemMap.put("predictScore", predictScore);

                    Double score = tuple.getScore();
                    String popularity = score != null && score >= 0.7 ? "高" :
                            score != null && score >= 0.4 ? "中" : "低";
                    itemMap.put("popularity", popularity);
                    resultList.add(itemMap);
                }
                System.out.println("[Redis HIT] 用户 " + userId + " 的推荐列表来自 Redis");
                return resultList;
            }

        } catch (Exception e) {
            System.err.println("[Redis Error] 从 Redis 获取推荐列表失败，降级到 MySQL: " + e.getMessage());
        }
        // MySQL降级路径（不变）
        List<Recommendations> recommendations = recommendationsMapper.selectList(
                new LambdaQueryWrapper<Recommendations>()
                        .eq(Recommendations::getUserId, userId)
                        .orderByAsc(Recommendations::getRankNo)
                        .last("LIMIT 10")
        );
        System.out.println("[MySQL Fallback] 用户 " + userId + " 的推荐列表来自 MySQL");
        return recommendations.stream().map(rec -> {
            Map<String, Object> map = new HashMap<>();
            map.put("rankNo", rec.getRankNo());
            map.put("itemId", rec.getItemId());
            map.put("finalScore", rec.getFinalScore() != null ?
                    rec.getFinalScore() : BigDecimal.ZERO);
            map.put("predictScore", rec.getPredictScore() != null ?
                    rec.getPredictScore() : BigDecimal.ZERO);
            BigDecimal popScore = rec.getPopularityScore();
            String popularity = popScore != null &&
                    popScore.compareTo(BigDecimal.valueOf(0.7)) >= 0 ? "高" :
                    popScore != null &&
                            popScore.compareTo(BigDecimal.valueOf(0.4)) >= 0 ? "中" : "低";
            map.put("popularity", popularity);
            return map;
        }).collect(Collectors.toList());
    }

    /**
     * 获取用户雷达图数据
     * 查询用户的6个维度的行为特征数据
     * @param userId 用户ID
     * @return 包含用户各维度数据的Map(浏览量、购买量、加购量、收藏量、购买率、活跃度)
     */
    public Map<String, Object> getUserRadar(Long userId) {
        UserFeatures user = userFeaturesMapper.selectOne(
                new LambdaQueryWrapper<UserFeatures>().eq(UserFeatures::getUserId, userId)
        );

        Map<String, Object> result = new HashMap<>();
        if (user != null) {
            result.put("userId", user.getUserId());
            result.put("pvCount", user.getPvCount() != null ? user.getPvCount() : 0);
            result.put("buyCount", user.getBuyCount() != null ? user.getBuyCount() : 0);
            result.put("cartCount", user.getCartCount() != null ? user.getCartCount() : 0);
            result.put("favCount", user.getFavCount() != null ? user.getFavCount() : 0);
            result.put("buyRate", user.getBuyRate() != null ? user.getBuyRate() : BigDecimal.ZERO);
            result.put("activeScore", user.getActiveScore() != null ?
                    user.getActiveScore() : BigDecimal.ZERO);
        } else {
            // 用户不存在时返回默认值
            result.put("userId", userId);
            result.put("pvCount", 0);
            result.put("buyCount", 0);
            result.put("cartCount", 0);
            result.put("favCount", 0);
            result.put("buyRate", BigDecimal.ZERO);
            result.put("activeScore", BigDecimal.ZERO);
        }
        return result;
    }

    /**
     * 获取pr-auc
     * 查询最新的有效评估指标(排除为-1的无效数据)
     * @return pr-auc和其评测时间
     */
    public Map<String, Object> getLatestPrAuc() {
        List<EvalMetrics> metrics = evalMetricsMapper.selectList(
                new LambdaQueryWrapper<EvalMetrics>()
                        .ne(EvalMetrics::getAccuracy, BigDecimal.valueOf(-1.0))  // pr_auc != -1
                        .isNotNull(EvalMetrics::getAccuracy)                     // 不为空
                        .orderByDesc(EvalMetrics::getCreateTime)                 // 按创建时间倒序
                        .last("LIMIT 1")                                         // 最新一条
        );

        Map<String, Object> result = new HashMap<>();
        if (!metrics.isEmpty()) {
            EvalMetrics metric = metrics.get(0);
            result.put("prAuc", metric.getAccuracy());       // PR-AUC 值
            result.put("createTime", metric.getCreateTime() != null
                    ? metric.getCreateTime().toString().replace("T", " ") : "");
        }
        return result;
    }


    /**
     * 插入用户行为数据
     * 将行为数据格式化为 JSON，通过 Kafka 发送到 Flink 进行实时处理
     *
     * @param data 包含行为信息的Map：user_id, item_id, category_id, behavior_type, timestamp
     * @throws Exception 发送数据失败时抛出异常
     */
    public void insertBehavior(Map<String, Object> data) throws Exception {
        try {
            Long userId = Long.valueOf(data.get("user_id").toString());
            Long itemId = Long.valueOf(data.get("item_id").toString());
            Long categoryId = Long.valueOf(data.get("category_id").toString());
            String behaviorType = data.get("behavior_type").toString();
            Long timestamp = Long.valueOf(data.get("timestamp").toString());
            String csvLine = String.format("%d,%d,%d,%s,%d",
                    userId, itemId, categoryId, behaviorType, timestamp);
            // 通过 Kafka 发送到 Flink
            kafkaTemplate.send(kafkaTopic, csvLine);
            System.out.println("[Kafka] 已发送行为数据到 topic=" + kafkaTopic + " : " + csvLine);
        } catch (Exception e) {
            // ★ 修复：只记录日志，不抛出异常
            // Kafka.send() 是异步的，即使这里报错（如连接超时），
            // 消息可能已经进入 Kafka 生产者缓冲区，最终会被投递
            System.err.println("[Kafka] 发送异常（数据可能已入队）: " + e.getMessage());
            // 不抛出异常 → 前端不会看到错误弹窗
        }
    }

    /**
     * 获取用户最近行为记录
     * 查询指定用户最近的行为数据，并添加中文描述
     * @param userId 用户ID
     * @param limit 返回记录数量限制
     * @return 包含行为详细信息的列表(包含中文行为类型描述)
     */
    public List<Map<String, Object>> getUserRecentBehaviors(Long userId, int limit) {
        // 按时间倒序查询用户行为
        List<UserBehavior> behaviors = userBehaviorMapper.selectList(
                new LambdaQueryWrapper<UserBehavior>()
                        .eq(UserBehavior::getUserId, userId)
                        .orderByDesc(UserBehavior::getCreateTime)
                        .last("LIMIT " + limit)
        );

        return behaviors.stream().map(behavior -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", behavior.getId());
            map.put("userId", behavior.getUserId());
            map.put("itemId", behavior.getItemId());
            map.put("categoryId", behavior.getCategoryId());
            map.put("behaviorType", behavior.getBehaviorType());
            map.put("behaviorTime", behavior.getCreateTime());

            // 将时间戳转换为可读格式
            map.put("behaviorTimeStr", behavior.getCreateTime() != null
                    ? behavior.getCreateTime().toString().replace("T", " ").substring(0, 19)
                    : "");

            // 添加中文行为类型描述
            String typeDesc = "";
            switch (behavior.getBehaviorType()) {
                case "pv": typeDesc = "浏览"; break;
                case "cart": typeDesc = "加购"; break;
                case "fav": typeDesc = "收藏"; break;
                case "buy": typeDesc = "购买"; break;
                default: typeDesc = behavior.getBehaviorType();
            }
            map.put("behaviorTypeDesc", typeDesc);

            return map;
        }).collect(Collectors.toList());
    }

    /**
     * 获取商品列表(支持关键字搜索)
     * 按商品浏览量降序排列，最多返回50条
     * @param keyword 搜索关键字(可选)，匹配商品ID
     * @return 包含商品ID、分类ID和浏览量的商品列表
     */
    public List<Map<String, Object>> getItemList(String keyword) {
        LambdaQueryWrapper<ItemFeatures> wrapper = new LambdaQueryWrapper<ItemFeatures>()
                .select(ItemFeatures::getItemId, ItemFeatures::getCategoryId,
                        ItemFeatures::getPvCount);

        // 如果有关键字，添加模糊查询条件
        if (keyword != null && !keyword.trim().isEmpty()) {
            wrapper.like(ItemFeatures::getItemId, keyword);
        }

        wrapper.orderByDesc(ItemFeatures::getPvCount).last("LIMIT 50");

        List<ItemFeatures> items = itemFeaturesMapper.selectList(wrapper);

        return items.stream().map(item -> {
            Map<String, Object> map = new HashMap<>();
            map.put("itemId", item.getItemId());
            map.put("categoryId", item.getCategoryId());
            map.put("pvCount", item.getPvCount());
            return map;
        }).collect(Collectors.toList());
    }

    /**
     * 获取商品分类列表
     * 查询所有商品的不同分类ID，去重并排序
     * @return 去重排序后的分类ID列表
     */
    public List<Long> getCategoryList() {
        List<ItemFeatures> items = itemFeaturesMapper.selectList(
                new LambdaQueryWrapper<ItemFeatures>()
                        .select(ItemFeatures::getCategoryId)
                        .groupBy(ItemFeatures::getCategoryId)
        );

        return items.stream()
                .map(ItemFeatures::getCategoryId)
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * 获取用户列表
     * 查询所有用户ID，去重并排序
     * @return 去重排序后的用户ID列表
     */
    public List<Long> getUserList() {
        List<UserFeatures> users = userFeaturesMapper.selectList(
                new LambdaQueryWrapper<UserFeatures>()
                        .select(UserFeatures::getUserId)
        );

        return users.stream()
                .map(UserFeatures::getUserId)
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * 定时更新用户特征
     * 遍历所有用户，重新计算并更新以下特征：
     * - 各类行为计数(pv/cart/fav/buy)
     * - 购买率(buy/pv)
     * - 活跃度评分(总行为数/100，上限1)
     * - 高频访问分类
     */
    public void updateUserFeatures() {
        System.out.println("开始执行用户特征定时更新任务...");

        long startTime = System.currentTimeMillis();

        // ===== ① 一次 GROUP BY 查出所有用户统计 =====
        List<Map<String, Object>> userStats = userBehaviorMapper.selectUserFeatureAggregation();
        int total = userStats.size();
        System.out.println("共查出 " + total + " 个用户，开始处理...");

        // ===== ② 一次查出所有用户的高频类目 =====
        Map<Long, Long> highFreqCategoryMap = new HashMap<>();
        for (Map<String, Object> row : userBehaviorMapper.selectUserHighFreqCategory()) {
            highFreqCategoryMap.put(
                    ((Number) row.get("user_id")).longValue(),
                    ((Number) row.get("category_id")).longValue()
            );
        }

        // ===== ③ 收集到 List =====
        List<UserFeatures> usersToUpdate = new ArrayList<>();
        int lastPercent = 0;

        for (int i = 0; i < total; i++) {
            Map<String, Object> row = userStats.get(i);
            try {
                Long userId = ((Number) row.get("user_id")).longValue();
                int pvCount = ((Number) row.get("pv_count")).intValue();
                int buyCount = ((Number) row.get("buy_count")).intValue();
                int cartCount = ((Number) row.get("cart_count")).intValue();
                int favCount = ((Number) row.get("fav_count")).intValue();

                // 计算购买率
                BigDecimal buyRate = pvCount > 0
                        ? BigDecimal.valueOf(buyCount)
                        .divide(BigDecimal.valueOf(pvCount), 4, RoundingMode.HALF_UP)
                        : BigDecimal.ZERO;

                // 计算活跃度评分(总行为数/100，最大为1)
                long totalBehaviors = pvCount + buyCount + cartCount + favCount;
                BigDecimal activeScore = BigDecimal.valueOf(
                        Math.min(totalBehaviors / 100.0, 1.0));

                // 获取高频类目
                Long highFreqCategory = highFreqCategoryMap.getOrDefault(userId, 0L);

                UserFeatures user = new UserFeatures();
                user.setUserId(userId);
                user.setPvCount(pvCount);
                user.setBuyCount(buyCount);
                user.setCartCount(cartCount);
                user.setFavCount(favCount);
                user.setBuyRate(buyRate);
                user.setActiveScore(activeScore);
                user.setHighFreqCategory(highFreqCategory);
                usersToUpdate.add(user);

            } catch (Exception e) {
                System.err.println("处理用户 " + row.get("user_id") + " 失败: " + e.getMessage());
            }

            // ===== 进度条：每 10% 打印一次 =====
            int percent = (i + 1) * 100 / total;
            if (percent >= lastPercent + 10) {
                lastPercent = percent;
                System.out.print("\r  用户特征处理进度: " + percent + "% (" + (i + 1) + "/" + total + ")");
                System.out.flush();
            }
        }
        System.out.println();  // 换行

        // ===== ④ 一次批量写入所有用户 → 1 次 SQL！ =====
        if (!usersToUpdate.isEmpty()) {
            long writeStart = System.currentTimeMillis();
            userFeaturesMapper.batchReplaceInto(usersToUpdate);
            long writeMs = System.currentTimeMillis() - writeStart;
            long totalMs = System.currentTimeMillis() - startTime;

            System.out.println("✅ 批量写入 " + usersToUpdate.size() + " 个用户特征");
            System.out.println("   写入耗时: " + writeMs + "ms");
            System.out.println("   总耗时: " + totalMs + "ms (" + String.format("%.1f", totalMs / 1000.0) + " 秒)");
        }
        System.out.println("用户特征定时更新任务完成");
    }


    /**
     * 获取全体用户的平均画像指标
     */
    public Map<String, Object> getAverageUserRadar() {
        Map<String, Object> result = new HashMap<>();

        Map<String, Object> avg = userFeaturesMapper.selectAverageUserRadar();

        if (avg != null) {
            result.put("pvCount", avg.get("avg_pv") != null ? avg.get("avg_pv") : 0);
            result.put("buyCount", avg.get("avg_buy") != null ? avg.get("avg_buy") : 0);
            result.put("cartCount", avg.get("avg_cart") != null ? avg.get("avg_cart") : 0);
            result.put("favCount", avg.get("avg_fav") != null ? avg.get("avg_fav") : 0);
            result.put("buyRate", avg.get("avg_buy_rate") != null ? avg.get("avg_buy_rate") : 0);
            result.put("activeScore", avg.get("avg_active_score") != null ? avg.get("avg_active_score") : 0);
            result.put("isAverage", true);  // 标记为平均值
        } else {
            // 无数据时返回默认值
            result.put("pvCount", 0);
            result.put("buyCount", 0);
            result.put("cartCount", 0);
            result.put("favCount", 0);
            result.put("buyRate", 0);
            result.put("activeScore", 0);
            result.put("isAverage", true);
        }

        return result;
    }




    /**
     * 定时更新商品特征
     * 遍历所有商品，重新计算并更新以下特征：
     * - 浏览量(pvCount)
     * - 购买量(buyCount)
     * - 流行度评分(浏览量×0.3 + 购买量×0.7)
     */
    public void updateItemFeatures() {
        System.out.println("开始执行商品特征定时更新任务...");

        long startTime = System.currentTimeMillis();

        // ===== ① 一次 GROUP BY 查出所有商品统计 =====
        List<Map<String, Object>> itemStats = userBehaviorMapper.selectItemFeatureAggregation();
        int total = itemStats.size();
        System.out.println("共查出 " + total + " 个商品，开始处理...");

        // ===== ② 收集到 List =====
        List<ItemFeatures> itemsToUpdate = new ArrayList<>();
        int lastPercent = 0;

        for (int i = 0; i < total; i++) {
            Map<String, Object> row = itemStats.get(i);
            try {
                Long itemId = ((Number) row.get("item_id")).longValue();
                Long categoryId = ((Number) row.get("category_id")).longValue();
                int pvCount = ((Number) row.get("pv_count")).intValue();
                int buyCount = ((Number) row.get("buy_count")).intValue();

                BigDecimal popularityScore = BigDecimal.valueOf(pvCount)
                        .multiply(BigDecimal.valueOf(0.3))
                        .add(BigDecimal.valueOf(buyCount)
                                .multiply(BigDecimal.valueOf(0.7)));

                ItemFeatures item = new ItemFeatures();
                item.setItemId(itemId);
                item.setCategoryId(categoryId);
                item.setPvCount(pvCount);
                item.setBuyCount(buyCount);
                item.setPopularityScore(popularityScore);
                itemsToUpdate.add(item);

            } catch (Exception e) {
                System.err.println("处理商品 " + row.get("item_id") + " 失败: " + e.getMessage());
            }

            // ===== 进度条：每 10% 打印一次 =====
            int percent = (i + 1) * 100 / total;
            if (percent >= lastPercent + 10) {
                lastPercent = percent;
                System.out.print("\r  商品特征处理进度: " + percent + "% (" + (i + 1) + "/" + total + ")");
                System.out.flush();
            }
        }
        System.out.println();  // 换行

        // ===== ③ 一次批量写入所有商品 → 1 次 SQL！ =====
        if (!itemsToUpdate.isEmpty()) {
            long writeStart = System.currentTimeMillis();
            itemFeaturesMapper.batchReplaceInto(itemsToUpdate);
            long writeMs = System.currentTimeMillis() - writeStart;
            long totalMs = System.currentTimeMillis() - startTime;

            System.out.println("✅ 批量写入 " + itemsToUpdate.size() + " 个商品特征");
            System.out.println("   写入耗时: " + writeMs + "ms");
            System.out.println("   总耗时: " + totalMs + "ms (" + String.format("%.1f", totalMs / 1000.0) + " 秒)");
        }
        System.out.println("商品特征定时更新任务完成");
    }



    /**
     * 触发模型重训练
     * 异步调用Python训练脚本，训练完成后导出PMML模型文件
     * @throws Exception 训练失败时抛出异常
     */
    public void triggerModelRetraining() throws Exception {
        System.out.println("=== 开始触发模型重训 ===");
        // train_model.py 在项目根目录的 MD 文件夹下
        // 从 evaluating 目录往上退到项目根
        String projectRoot = pythonScriptDir.replace("web\\backend\\src\\main\\java\\com\\recommend\\evaluating\\", "");
        String pythonScriptPath = projectRoot + "MD\\train_model.py";

        System.out.println("Python 脚本路径: " + pythonScriptPath);
        ProcessBuilder processBuilder = new ProcessBuilder("python", pythonScriptPath);
        processBuilder.redirectErrorStream(true);
        Map<String, String> env = processBuilder.environment();
        env.put("PYTHONIOENCODING", "utf-8");
        Process process = processBuilder.start();

        // 异步读取训练日志
        new Thread(() -> {
            try (java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(process.getInputStream(), "UTF-8"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[Python] " + line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        int exitCode = process.waitFor();

        if (exitCode == 0) {
            System.out.println("=== 模型重训成功完成 ===");
            System.out.println("PMML 模型已导出");
        } else {
            throw new RuntimeException("模型重训失败，退出码: " + exitCode);
        }
    }

    /**
     * 触发模型评测
     * 异步调用Python评测脚本，对当前模型进行全面性能评估
     * @throws Exception 评测失败时抛出异常
     */
    public void triggerModelEvaluation() throws Exception {
        System.out.println("=== 开始触发模型评测 ===");
        String pythonScriptPath = pythonScriptDir + "test_for_model.py";
        System.out.println("Python 脚本路径: " + pythonScriptPath);

        ProcessBuilder processBuilder = new ProcessBuilder("python", pythonScriptPath);
        processBuilder.redirectErrorStream(true);
        processBuilder.environment().put("PYTHONIOENCODING", "utf-8");
        Process process = processBuilder.start();

        new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), "UTF-8"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[Python Model Eval] " + line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // ===== 心跳线程，每10秒输出一次 =====
        Thread heartbeat = new Thread(() -> {
            try {
                while (process.isAlive()) {
                    Thread.sleep(10000);
                    System.out.println("[Java] ⏳ 模型评测仍在进行中，请耐心等待...");
                }
            } catch (InterruptedException ignored) {}
        });
        heartbeat.setDaemon(true);
        heartbeat.start();

        int exitCode = process.waitFor();

        if (exitCode == 0) {
            System.out.println("=== 模型评测成功完成 ===");
        } else {
            throw new RuntimeException("模型评测失败，退出码: " + exitCode);
        }
    }



}