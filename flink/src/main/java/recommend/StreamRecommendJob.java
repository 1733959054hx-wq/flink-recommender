package recommend;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.HasProbability;
import org.jpmml.evaluator.InputField;
import org.jpmml.model.PMMLUtil;
import recommend.optimizer.DataSkewOptimizer;
import recommend.sink.RedisSink;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink 实时推荐系统 - 流批统一入口（带自定义算子名称版）
 * 算子拓扑：
 * Kafka Source -> [P5 数据清洗] -> 数据解析 -> [实时数据落库]
 *   -> [P3 滑动窗口] -> [P4 数据倾斜优化] -> [特征合并 + 冷启动 + Redis缓存]
 *   -> [PMML预测] -> [TopK排序] -> [MySQL Sink + Redis Sink + 控制台输出]
 */
public class StreamRecommendJob {
    // Kafka配置
    private static final String KAFKA_BOOTSTRAP_SERVERS = "192.168.88.161:9092";
    private static final String KAFKA_TOPIC = "user-behavior-topic";
    private static final String KAFKA_GROUP_ID = "flink-recommend-group";
    // MySQL配置
    private static final String JDBC_URL = "jdbc:mysql://192.168.88.8:3306/recommend_db?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true";
    private static final String JDBC_USER = "flinkpy";
    private static final String JDBC_PASSWORD = "123456";
    // Redis配置
    private static final String REDIS_HOST = "192.168.88.8";
    private static final int REDIS_PORT = 6379;
    private static final String REDIS_PASSWORD = "";
    private static final int UF_TTL_SECONDS = 7200;
    private static final int IF_TTL_SECONDS = 86400;
    // PMML模型路径
    private static final String PMML_MODEL_PATH = "recommend_model_champion.pmml";
    // 冷启动默认值
    private static final BigDecimal DEFAULT_BUY_RATE = new BigDecimal("0.05");
    private static final BigDecimal DEFAULT_ACTIVE_SCORE = new BigDecimal("1.0");
    private static final BigDecimal DEFAULT_POPULARITY = new BigDecimal("0.5");
    // TopK参数
    private static final int TOP_K = 10;
    private static final long TOPK_FLUSH_INTERVAL_MS = 5000;
    // 滑动窗口配置
    private static final long WINDOW_SIZE_SECONDS = 30;
    private static final long WINDOW_SLIDE_SECONDS = 10;
    // 时间戳边界
    private static final long TS_LOWER = 1451606400L;
    private static final long TS_UPPER = 1893456000L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        printBanner();

        // ========== Step 1: Kafka数据源 ==========
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(KAFKA_TOPIC)
                .setGroupId(KAFKA_GROUP_ID)
//                .setStartingOffsets(OffsetsInitializer.earliest())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // ========== Step 2: P5数据清洗 ==========
        DataStream<String> cleanedStream = rawStream
                .filter(value -> {
                    if (value == null || value.trim().isEmpty()) return false;
                    String[] f = value.trim().split(",");
                    if (f.length != 5) return false;
                    try {
                        long uid = Long.parseLong(f[0].trim());
                        long iid = Long.parseLong(f[1].trim());
                        long cid = Long.parseLong(f[2].trim());
                        String bt = f[3].trim().toLowerCase();
                        long ts = Long.parseLong(f[4].trim());
                        if (uid <= 0 || iid <= 0 || cid <= 0) return false;
                        if (!"pv".equals(bt) && !"buy".equals(bt) && !"cart".equals(bt) && !"fav".equals(bt)) return false;
                        return ts >= TS_LOWER && ts <= TS_UPPER;
                    } catch (NumberFormatException e) { return false; }
                })
                .uid("data-clean-filter")
                .name("P5-数据清洗-过滤")
                .map(value -> {
                    String[] f = value.trim().split(",");
                    try {
                        long ts = Long.parseLong(f[4].trim());
                        if (ts > 2000000000L && ts >= 1451606400000L) {
                            f[4] = String.valueOf(ts / 1000);
                            return String.join(",", f);
                        }
                    } catch (NumberFormatException ignored) {}
                    return value;
                })
                .uid("data-clean-time-normalize")
                .name("P5-数据清洗-时间归一化");

        // ========== Step 3: 数据解析 ==========
        DataStream<UserBehavior> behaviorStream = cleanedStream
                .map(value -> {
                    String[] f = value.split(",");
                    return new UserBehavior(
                            Long.parseLong(f[0].trim()),
                            Long.parseLong(f[1].trim()),
                            Long.parseLong(f[2].trim()),
                            f[3].trim().toLowerCase(),
                            Long.parseLong(f[4].trim())
                    );
                })
                .uid("data-parse")
                .name("数据解析-String转UserBehavior");

        // ========== Step 3.5: 实时数据微批落库 ==========
        behaviorStream
                .addSink(JdbcSink.sink(
                        // 将 timestamp 改为 behavior_time
                        "INSERT INTO user_behavior (user_id, item_id, category_id, behavior_type, behavior_time, create_time) VALUES (?, ?, ?, ?, ?, NOW())",
                        (ps, b) -> {
                            ps.setLong(1, b.userId);
                            ps.setLong(2, b.itemId);
                            ps.setLong(3, b.categoryId);
                            ps.setString(4, b.behaviorType);
                            ps.setLong(5, b.timestamp);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(500)
                                .withBatchIntervalMs(1000)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(JDBC_URL)
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername(JDBC_USER)
                                .withPassword(JDBC_PASSWORD)
                                .build()
                ))
                .uid("sink-mysql-user-behavior")
                .name("Sink: MySQL-用户行为落库");

        // ========== Step 4: P3滑动窗口聚合(给模型 pvCount5min和 isCartOrFav 参数) ==========
//        DataStream<RealtimeFeature> featureStream = behaviorStream
//                .keyBy(b -> b.userId + "_" + b.itemId)
//                .window(SlidingProcessingTimeWindows.of(
//                        Time.seconds(WINDOW_SIZE_SECONDS),
//                        Time.seconds(WINDOW_SLIDE_SECONDS)
//                ))
//                .reduce(
//                        (v1, v2) -> new UserBehavior(v1.userId, v1.itemId, v2.categoryId, v1.behaviorType, v2.timestamp),
//                        new ProcessWindowFunction<UserBehavior, RealtimeFeature, String, TimeWindow>() {
//                            @Override
//                            public void process(String key, Context ctx, Iterable<UserBehavior> elements, Collector<RealtimeFeature> out) {
//                                String[] p = key.split("_");
//                                long uid = Long.parseLong(p[0]), iid = Long.parseLong(p[1]);
//                                int pv = 0, cof = 0;
//                                long cid = 0;
//                                for (UserBehavior b : elements) {
//                                    pv++;
//                                    if ("cart".equals(b.behaviorType) || "fav".equals(b.behaviorType)) cof = 1;
//                                    cid = b.categoryId;
//                                }
//                                out.collect(new RealtimeFeature(uid, iid, cid, pv, cof));
//                            }
//                        }
//                )
//                .uid("p3-sliding-window")
//                .name("P3-滑动窗口聚合(30s/10s)");
//        原代码使用 `reduce` + `ProcessWindowFunction` 组合进行窗口聚合，存在严重的数据丢失问题：
//        1. reduce函数中硬编码 `v1.behaviorType`，导致cart/fav等重要行为类型被丢弃
//        2. reduce函数只保留第一个元素的behaviorType，后续行为类型全部丢失
//        3. 导致窗口内PV计数错误（实际应该统计所有PV，但reduce后只剩1条记录）
//        4. isCartOrFav标识永远为0，无法捕获用户的购物车/收藏意图
        DataStream<RealtimeFeature> featureStream = behaviorStream
                .keyBy(b -> b.userId + "_" + b.itemId)
                .window(SlidingProcessingTimeWindows.of(
                        Time.seconds(WINDOW_SIZE_SECONDS),
                        Time.seconds(WINDOW_SLIDE_SECONDS)
                ))
                .aggregate(
                        new AggregateFunction<UserBehavior, WindowAccumulator, RealtimeFeature>() {
                            @Override
                            public WindowAccumulator createAccumulator() {
                                return new WindowAccumulator();
                            }

                            @Override
                            public WindowAccumulator add(UserBehavior value, WindowAccumulator accumulator) {
                                // 更新用户和商品ID
                                accumulator.userId = value.userId;
                                accumulator.itemId = value.itemId;

                                // PV计数+1
                                accumulator.pvCount++;

                                // 检测购物车或收藏行为
                                if ("cart".equals(value.behaviorType) || "fav".equals(value.behaviorType)) {
                                    accumulator.hasCartOrFav = true;
                                }

                                // 使用最新时间戳的categoryId
                                if (value.timestamp > accumulator.latestTimestamp) {
                                    accumulator.categoryId = value.categoryId;
                                    accumulator.latestTimestamp = value.timestamp;
                                }

                                return accumulator;
                            }

                            @Override
                            public RealtimeFeature getResult(WindowAccumulator accumulator) {
                                return new RealtimeFeature(
                                        accumulator.userId,
                                        accumulator.itemId,
                                        accumulator.categoryId,
                                        accumulator.pvCount,
                                        accumulator.hasCartOrFav ? 1 : 0
                                );
                            }

                            @Override
                            public WindowAccumulator merge(WindowAccumulator a, WindowAccumulator b) {
                                WindowAccumulator merged = new WindowAccumulator();
                                merged.userId = a.userId;
                                merged.itemId = a.itemId;

                                // 合并PV计数
                                merged.pvCount = a.pvCount + b.pvCount;

                                // 任一累加器有cart/fav行为，最终结果就为true
                                merged.hasCartOrFav = a.hasCartOrFav || b.hasCartOrFav;

                                // 保留最新的categoryId
                                if (b.latestTimestamp > a.latestTimestamp) {
                                    merged.categoryId = b.categoryId;
                                    merged.latestTimestamp = b.latestTimestamp;
                                } else {
                                    merged.categoryId = a.categoryId;
                                    merged.latestTimestamp = a.latestTimestamp;
                                }

                                return merged;
                            }
                        }
                )
                .uid("p3-sliding-window")
                .name("P3-滑动窗口聚合(30s/10s)-修复版");
        // ========== Step P4: 数据倾斜优化 ==========
        DataStream<Tuple2<Long, Integer>> hotItemStream = behaviorStream
                .map(b -> Tuple3.of(b.itemId, b.userId, b.behaviorType))
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.LONG,
                        org.apache.flink.api.common.typeinfo.Types.LONG,
                        org.apache.flink.api.common.typeinfo.Types.STRING
                ))
                .uid("p4-prepare-hot-items")
                .name("P4-准备热商品数据")
                .filter(t -> "pv".equals(t.f2))
                .uid("p4-filter-pv")
                .name("P4-过滤PV行为")
                .map(t -> Tuple2.of(t.f0, 1))
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.LONG,
                        org.apache.flink.api.common.typeinfo.Types.INT
                ))
                .uid("p4-map-to-counter")
                .name("P4-转换为计数器");

        DataStream<Tuple2<Long, Integer>> hotItems = DataSkewOptimizer.twoPhaseAggregate(
                hotItemStream, 300, 60
        );

        hotItems
                .map(t -> {
                    System.out.println("[P4热商品] itemId=" + t.f0 + " PV=" + t.f1);
                    return t;
                })
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.LONG,
                        org.apache.flink.api.common.typeinfo.Types.INT
                ))
                .uid("p4-hot-items-print")
                .name("P4-热商品控制台输出");

        // ========== Step 5: 特征合并 + PMML预测 ==========
        DataStream<Tuple5<Long, Long, BigDecimal, BigDecimal, BigDecimal>> predictionStream = featureStream
                .keyBy(f -> f.userId)
                .map(new FeatureMergeAndPredictFunction())
                .uid("feature-merge-predict")
                .name("特征合并+PMML预测(Redis缓存)");

        // ========== Step 6: TopK排序 ==========
        DataStream<Recommendation> topKStream = predictionStream
                .keyBy(t -> t.f0)
                .process(new TopKProcessFunction(TOP_K, TOPK_FLUSH_INTERVAL_MS))
                .uid("topk-sort")
                .name("TopK排序处理");

        // ========== Step 7: 控制台输出 ==========
        topKStream.print("Recommendation>>").uid("sink-print-console").name("Sink: Print-控制台输出推荐结果");

        // ========== Step 8: MySQL Sink ==========
        topKStream
                .addSink(JdbcSink.sink(
                        // 现在更新时不会改变 create_time了
                        "INSERT INTO recommendations (user_id, item_id, predict_score, popularity_score, final_score, rank_no, recommend_type, create_time) " +
                                "VALUES (?, ?, ?, ?, ?, ?, 'realtime', NOW()) " +
                                "ON DUPLICATE KEY UPDATE predict_score=VALUES(predict_score), popularity_score=VALUES(popularity_score), " +
                                "final_score=VALUES(final_score), rank_no=VALUES(rank_no)",
                        (ps, r) -> {
                            ps.setLong(1, r.userId);
                            ps.setLong(2, r.itemId);
                            ps.setBigDecimal(3, r.predictScore);
                            ps.setBigDecimal(4, r.popularityScore);
                            ps.setBigDecimal(5, r.finalScore);
                            ps.setInt(6, r.rankNo);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)
                                .withBatchIntervalMs(200)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(JDBC_URL)
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername(JDBC_USER)
                                .withPassword(JDBC_PASSWORD)
                                .build()
                ))
                .uid("sink-mysql-recommendations")
                .name("Sink: MySQL-推荐结果入库");

        // ========== Step 9: Redis Sink ==========
        topKStream
                .addSink(new RedisSink())
                .uid("sink-redis-cache")
                .name("Sink: Redis-推荐缓存");

        printStartupInfo();
        env.execute("Flink Real-time Recommendation System (Named Operators)");
    }

    // ==================== 数据模型类 ====================

    /**
     * 用户行为数据模型
     */
    public static class UserBehavior {
        public long userId, itemId, categoryId, timestamp;
        public String behaviorType;

        public UserBehavior() {}

        public UserBehavior(long userId, long itemId, long categoryId, String behaviorType, long timestamp) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behaviorType = behaviorType;
            this.timestamp = timestamp;
        }
    }

    /**
     * 实时特征数据模型
     */
    public static class RealtimeFeature {
        public long userId, itemId, categoryId;
        public int pvCount5min, isCartOrFav;

        public RealtimeFeature() {}

        public RealtimeFeature(long userId, long itemId, long categoryId, int pvCount5min, int isCartOrFav) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.pvCount5min = pvCount5min;
            this.isCartOrFav = isCartOrFav;
        }
    }

    /**
     * 推荐结果数据模型
     */
    public static class Recommendation {
        public long userId, itemId;
        public BigDecimal predictScore, popularityScore, finalScore;
        public int rankNo;

        public Recommendation() {}

        public Recommendation(long userId, long itemId, BigDecimal predictScore,
                              BigDecimal popularityScore, BigDecimal finalScore, int rankNo) {
            this.userId = userId;
            this.itemId = itemId;
            this.predictScore = predictScore;
            this.popularityScore = popularityScore;
            this.finalScore = finalScore;
            this.rankNo = rankNo;
        }
    }

    // ==================== 特征合并+PMML预测函数 ====================

    /**
     * 特征合并与PMML预测（Redis缓存优化版）
     * 功能：
     * 1. 从Redis/MySQL获取用户特征和商品特征
     * 2. 冷启动处理
     * 3. PMML模型预测
     * 4. 综合评分计算
     */
//    private static class FeatureMergeAndPredictFunction
//            extends RichMapFunction<RealtimeFeature, Tuple5<Long, Long, BigDecimal, BigDecimal, BigDecimal>> {
//
//        private transient Connection conn;
//        private transient Evaluator pmmlEvaluator;
//        private transient JedisPool jedisPool;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//            // 初始化MySQL连接
//            Class.forName("com.mysql.cj.jdbc.Driver");
//            conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
//
//            // 初始化Redis连接池
//            JedisPoolConfig poolConfig = new JedisPoolConfig();
//            poolConfig.setMaxTotal(10);
//            poolConfig.setMaxIdle(5);
//            poolConfig.setMinIdle(2);
//            poolConfig.setTestOnBorrow(true);
////            jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 2000, REDIS_PASSWORD);
//            jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 2000);
//            // 加载PMML模型
//            InputStream is = getClass().getClassLoader().getResourceAsStream(PMML_MODEL_PATH);
//            if (is == null) {
//                throw new RuntimeException("模型文件未找到: " + PMML_MODEL_PATH);
//            }
//            try {
//                PMML pmml = PMMLUtil.unmarshal(is);
//                Model model = pmml.getModels().get(0);
//                pmmlEvaluator = new org.jpmml.evaluator.ModelEvaluatorBuilder(pmml, model).build();
//            } finally {
//                if (is != null) is.close();
//            }
//        }
//
//        @Override
//        public void close() throws Exception {
//            if (jedisPool != null) jedisPool.close();
//            if (conn != null && !conn.isClosed()) conn.close();
//            super.close();
//        }
//
//        @Override
//        public Tuple5<Long, Long, BigDecimal, BigDecimal, BigDecimal> map(RealtimeFeature rf) throws Exception {
//            try (Jedis jedis = jedisPool.getResource()) {
//                // ===== 1. 查询用户特征（Redis缓存优先） =====
//                BigDecimal buyRate, activeScore;
//                long highFreqCategory;
//                String ufKey = "uf:" + rf.userId;
//                Map<String, String> ufCache = jedis.hgetAll(ufKey);
//
//                if (!ufCache.isEmpty()) {
//                    // Redis缓存命中
//                    buyRate = new BigDecimal(ufCache.get("buy_rate"));
//                    activeScore = new BigDecimal(ufCache.get("active_score"));
//                    highFreqCategory = Long.parseLong(ufCache.get("high_freq_category"));
//                } else {
//                    // Redis未命中，查询MySQL
//                    PreparedStatement ps = conn.prepareStatement(
//                            "SELECT buy_rate, active_score, high_freq_category FROM user_features WHERE user_id = ?"
//                    );
//                    ps.setLong(1, rf.userId);
//                    ResultSet rs = ps.executeQuery();
//
//                    if (rs.next()) {
//                        buyRate = rs.getBigDecimal("buy_rate");
//                        activeScore = rs.getBigDecimal("active_score");
//                        highFreqCategory = rs.getLong("high_freq_category");
//                    } else {
//                        // 冷启动：使用默认值
//                        buyRate = DEFAULT_BUY_RATE;
//                        activeScore = DEFAULT_ACTIVE_SCORE;
//                        highFreqCategory = rf.categoryId;
//                    }
//                    rs.close();
//                    ps.close();
//
//                    // 写入Redis缓存（冷启动数据不缓存）
//                    if (buyRate != DEFAULT_BUY_RATE) {
//                        Map<String, String> fields = new LinkedHashMap<>();
//                        fields.put("buy_rate", buyRate.toPlainString());
//                        fields.put("active_score", activeScore.toPlainString());
//                        fields.put("high_freq_category", String.valueOf(highFreqCategory));
//                        jedis.hmset(ufKey, fields);
//                        jedis.expire(ufKey, UF_TTL_SECONDS);
//                    }
//                }
//
//                // 计算类目匹配
//                int categoryMatch = (rf.categoryId == highFreqCategory) ? 1 : 0;
//
//                // ===== 2. PMML预测 =====
//                BigDecimal predictScore = predictWithPMML(
//                        buyRate.doubleValue(),
//                        activeScore.doubleValue(),
//                        rf.pvCount5min,
//                        rf.isCartOrFav,
//                        categoryMatch
//                );
//
//                // ===== 3. 查询商品热度（Redis缓存优先） =====
//                BigDecimal popularityScore;
//                String ifKey = "if:" + rf.itemId;
//                String popCache = jedis.hget(ifKey, "popularity_score");
//
//                if (popCache != null) {
//                    // Redis缓存命中
//                    popularityScore = new BigDecimal(popCache);
//                } else {
//                    // 查询MySQL
//                    PreparedStatement ps2 = conn.prepareStatement(
//                            "SELECT popularity_score FROM item_features WHERE item_id = ?"
//                    );
//                    ps2.setLong(1, rf.itemId);
//                    ResultSet rs2 = ps2.executeQuery();
//
//                    popularityScore = rs2.next() && rs2.getBigDecimal("popularity_score") != null
//                            ? rs2.getBigDecimal("popularity_score")
//                            : DEFAULT_POPULARITY;
//                    rs2.close();
//                    ps2.close();
//
//                    // 写入Redis缓存
//                    jedis.hset(ifKey, "popularity_score", popularityScore.toPlainString());
//                    jedis.expire(ifKey, IF_TTL_SECONDS);
//                }
//
//                // ===== 4. 计算综合得分 =====
//                BigDecimal finalScore = predictScore.multiply(new BigDecimal("0.7"))
//                        .add(popularityScore.multiply(new BigDecimal("0.3")));
//
//                return Tuple5.of(rf.userId, rf.itemId, predictScore, popularityScore, finalScore);
//            }
//        }
//
//        /**
//         * PMML模型预测
//         */
//        private BigDecimal predictWithPMML(double buyRate, double activeScore,
//                                           int pvCount5min, int isCartOrFav,
//                                           int categoryMatch) throws Exception {
//            Map<String, FieldValue> args = new LinkedHashMap<>();
//            for (InputField field : pmmlEvaluator.getInputFields()) {
//                String name = field.getName();
//                double val;
//                switch (name) {
//                    case "buy_rate": val = buyRate; break;
//                    case "active_score": val = activeScore; break;
//                    case "pv_count_5min": val = pvCount5min; break;
//                    case "is_cart_or_fav": val = isCartOrFav; break;
//                    case "category_match": val = categoryMatch; break;
//                    default: val = 0.0;
//                }
//                args.put(name, field.prepare(val));
//            }
//
//            Map<String, ?> results = pmmlEvaluator.evaluate(args);
//            double prob = 0.0;
//            Object obj = results.get("probability(1)");
//            if (obj instanceof HasProbability) {
//                prob = ((HasProbability) obj).getProbability("1");
//            } else if (obj instanceof Number) {
//                prob = ((Number) obj).doubleValue();
//            }
//            prob = Math.max(0.0, Math.min(1.0, prob));
//            return BigDecimal.valueOf(prob);
//        }
//    }
    private static class FeatureMergeAndPredictFunction
            extends RichMapFunction<RealtimeFeature, Tuple5<Long, Long, BigDecimal, BigDecimal, BigDecimal>> {

        private transient Connection conn;
        private transient Evaluator pmmlEvaluator;
        private transient JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化MySQL连接
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);

            // 初始化Redis连接池 - 增加连接数
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(20);      // 增加最大连接数
            poolConfig.setMaxIdle(10);
            poolConfig.setMinIdle(5);
            poolConfig.setMaxWaitMillis(5000); // 设置等待超时
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestWhileIdle(true);
            poolConfig.setBlockWhenExhausted(true);
            jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 2000);

            // 加载PMML模型
            InputStream is = getClass().getClassLoader().getResourceAsStream(PMML_MODEL_PATH);
            if (is == null) {
                throw new RuntimeException("模型文件未找到: " + PMML_MODEL_PATH);
            }
            try {
                PMML pmml = PMMLUtil.unmarshal(is);
                Model model = pmml.getModels().get(0);
                pmmlEvaluator = new org.jpmml.evaluator.ModelEvaluatorBuilder(pmml, model).build();
            } finally {
                if (is != null) {
                    try { is.close(); } catch (Exception e) { /* ignore */ }
                }
            }
        }

        @Override
        public void close() throws Exception {
            // 先关闭 Redis 连接池
            if (jedisPool != null && !jedisPool.isClosed()) {
                jedisPool.close();
            }
            // 再关闭 MySQL 连接
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
            super.close();
        }

        @Override
        public Tuple5<Long, Long, BigDecimal, BigDecimal, BigDecimal> map(RealtimeFeature rf) throws Exception {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();

                // ===== 1. 查询用户特征（Redis缓存优先） =====
                BigDecimal buyRate, activeScore;
                long highFreqCategory;
                String ufKey = "uf:" + rf.userId;
                Map<String, String> ufCache = jedis.hgetAll(ufKey);

                if (!ufCache.isEmpty()) {
                    // Redis缓存命中
                    buyRate = new BigDecimal(ufCache.get("buy_rate"));
                    activeScore = new BigDecimal(ufCache.get("active_score"));
                    highFreqCategory = Long.parseLong(ufCache.get("high_freq_category"));
                } else {
                    // Redis未命中，查询MySQL
                    buyRate = queryUserFeatureFromMySQL(rf.userId, rf.categoryId, jedis);
                    activeScore = DEFAULT_ACTIVE_SCORE;
                    highFreqCategory = rf.categoryId;

                    // 如果buyRate是默认值，重新查询完整的用户特征
                    if (buyRate == DEFAULT_BUY_RATE) {
                        // 已经使用默认值，不需要额外处理
                    }
                }

                // 计算类目匹配
                int categoryMatch = (rf.categoryId == highFreqCategory) ? 1 : 0;

                // ===== 2. PMML预测 =====
                BigDecimal predictScore = predictWithPMML(
                        buyRate.doubleValue(),
                        activeScore.doubleValue(),
                        rf.pvCount5min,
                        rf.isCartOrFav,
                        categoryMatch
                );

                // ===== 3. 查询商品热度（Redis缓存优先） =====
                BigDecimal popularityScore = queryItemPopularityFromRedis(rf.itemId, jedis);

                // ===== 4. 计算综合得分 =====
                BigDecimal finalScore = predictScore.multiply(new BigDecimal("0.7"))
                        .add(popularityScore.multiply(new BigDecimal("0.3")))
                        .setScale(12, RoundingMode.HALF_UP);//限制精度

                return Tuple5.of(rf.userId, rf.itemId, predictScore, popularityScore, finalScore);

            } finally {
                // 关键：确保 Jedis 连接归还到连接池
                if (jedis != null) {
                    try {
                        jedis.close();
                    } catch (Exception e) {
                        System.err.println("[FeatureMerge] 关闭Jedis连接失败: " + e.getMessage());
                    }
                }
            }
        }

        /**
         * 从MySQL查询用户特征（带资源自动释放）
         */
        private BigDecimal queryUserFeatureFromMySQL(long userId, long categoryId, Jedis jedis) throws Exception {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ps = conn.prepareStatement(
                        "SELECT buy_rate, active_score, high_freq_category FROM user_features WHERE user_id = ?"
                );
                ps.setLong(1, userId);
                rs = ps.executeQuery();

                BigDecimal buyRate;
                BigDecimal activeScore;
                long highFreqCategory;

                if (rs.next()) {
                    buyRate = rs.getBigDecimal("buy_rate");
                    activeScore = rs.getBigDecimal("active_score");
                    highFreqCategory = rs.getLong("high_freq_category");

                    // 写入Redis缓存（冷启动数据不缓存）
                    Map<String, String> fields = new LinkedHashMap<>();
                    fields.put("buy_rate", buyRate.toPlainString());
                    fields.put("active_score", activeScore.toPlainString());
                    fields.put("high_freq_category", String.valueOf(highFreqCategory));
                    jedis.hmset("uf:" + userId, fields);
                    jedis.expire("uf:" + userId, UF_TTL_SECONDS);
                } else {
                    // 冷启动：使用默认值
                    buyRate = DEFAULT_BUY_RATE;
                    activeScore = DEFAULT_ACTIVE_SCORE;
                    highFreqCategory = categoryId;
                }

                return buyRate;

            } finally {
                // 关键：在 finally 块中释放资源
                if (rs != null) {
                    try { rs.close(); } catch (Exception e) { /* ignore */ }
                }
                if (ps != null) {
                    try { ps.close(); } catch (Exception e) { /* ignore */ }
                }
            }
        }

        /**
         * 从Redis查询商品热度（缓存未命中则查MySQL）
         */
        private BigDecimal queryItemPopularityFromRedis(long itemId, Jedis jedis) throws Exception {
            String ifKey = "if:" + itemId;
            String popCache = jedis.hget(ifKey, "popularity_score");

            if (popCache != null) {
                return new BigDecimal(popCache);
            }

            // Redis未命中，查询MySQL
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ps = conn.prepareStatement(
                        "SELECT popularity_score FROM item_features WHERE item_id = ?"
                );
                ps.setLong(1, itemId);
                rs = ps.executeQuery();

                BigDecimal popularityScore;
                if (rs.next() && rs.getBigDecimal("popularity_score") != null) {
                    popularityScore = rs.getBigDecimal("popularity_score");
                } else {
                    popularityScore = DEFAULT_POPULARITY;
                }

                // 写入Redis缓存
                jedis.hset(ifKey, "popularity_score", popularityScore.toPlainString());
                jedis.expire(ifKey, IF_TTL_SECONDS);

                return popularityScore;

            } finally {
                if (rs != null) {
                    try { rs.close(); } catch (Exception e) { /* ignore */ }
                }
                if (ps != null) {
                    try { ps.close(); } catch (Exception e) { /* ignore */ }
                }
            }
        }

        /**
         * PMML模型预测
         */
        private BigDecimal predictWithPMML(double buyRate, double activeScore,
                                           int pvCount5min, int isCartOrFav,
                                           int categoryMatch) throws Exception {
            Map<String, FieldValue> args = new LinkedHashMap<>();
            for (InputField field : pmmlEvaluator.getInputFields()) {
                String name = field.getName();
                double val;
                switch (name) {
                    case "buy_rate": val = buyRate; break;
                    case "active_score": val = activeScore; break;
                    case "pv_count_5min": val = pvCount5min; break;
                    case "is_cart_or_fav": val = isCartOrFav; break;
                    case "category_match": val = categoryMatch; break;
                    default: val = 0.0;
                }
                args.put(name, field.prepare(val));
            }

            Map<String, ?> results = pmmlEvaluator.evaluate(args);
            double prob = 0.0;
            Object obj = results.get("probability(1)");
            if (obj instanceof HasProbability) {
                prob = ((HasProbability) obj).getProbability("1");
            } else if (obj instanceof Number) {
                prob = ((Number) obj).doubleValue();
            }
            prob = Math.max(0.0, Math.min(1.0, prob));
            return BigDecimal.valueOf(prob);
        }
    }


    // ==================== TopK排序处理函数 ====================

    /**
     * TopK排序处理器
     * 功能：
     * 1. 维护每个用户的最优推荐结果
     * 2. 定时输出TopK推荐
     * 3. 按综合得分排序
     */
    private static class TopKProcessFunction
            extends KeyedProcessFunction<Long, Tuple5<Long, Long, BigDecimal, BigDecimal, BigDecimal>, Recommendation> {

        private final int topK;
        private final long flushIntervalMs;
        private transient MapState<Long, Recommendation> bestRecs;
        private transient ValueState<Long> timerState;

        public TopKProcessFunction(int topK, long flushIntervalMs) {
            this.topK = topK;
            this.flushIntervalMs = flushIntervalMs;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            bestRecs = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("topk-recs", Long.class, Recommendation.class)
            );
            timerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("topk-timer", Long.class)
            );
        }

        @Override
        public void processElement(
                Tuple5<Long, Long, BigDecimal, BigDecimal, BigDecimal> v,
                Context ctx,
                Collector<Recommendation> out) throws Exception {

            // 检查是否比已存在的推荐更好
            Recommendation existing = bestRecs.get(v.f1);
            if (existing == null || v.f4.compareTo(existing.finalScore) > 0) {
                bestRecs.put(v.f1, new Recommendation(v.f0, v.f1, v.f2, v.f3, v.f4, 0));
            }

            // 注册定时器
            if (timerState.value() == null) {
                long t = ctx.timerService().currentProcessingTime() + flushIntervalMs;
                ctx.timerService().registerProcessingTimeTimer(t);
                timerState.update(t);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Recommendation> out) throws Exception {
            timerState.clear();

            // 收集所有推荐结果
            List<Recommendation> list = new ArrayList<>();
            for (Recommendation r : bestRecs.values()) {
                list.add(r);
            }

            if (list.isEmpty()) return;

            // 按综合得分降序排序
            list.sort((a, b) -> b.finalScore.compareTo(a.finalScore));

            // 输出TopK
            int actualTopK = Math.min(topK, list.size());
            int rank = 1;
            for (Recommendation r : list.subList(0, actualTopK)) {
                r.rankNo = rank++;
                out.collect(r);
            }

            // 注册下一个定时器
            long next = ctx.timerService().currentProcessingTime() + flushIntervalMs;
            ctx.timerService().registerProcessingTimeTimer(next);
            timerState.update(next);
        }
    }
// ==================== 窗口聚合累加器 ====================

    /**
     * 滑动窗口聚合累加器
     * 用于在窗口内累加用户行为数据，避免数据丢失
     */
    public static class WindowAccumulator {
        public long userId;
        public long itemId;
        public long categoryId;
        public int pvCount = 0;
        public boolean hasCartOrFav = false;
        public long latestTimestamp = 0;

        public WindowAccumulator() {}

        @Override
        public String toString() {
            return String.format(
                    "WindowAccumulator{userId=%d, itemId=%d, categoryId=%d, pvCount=%d, hasCartOrFav=%b, latestTimestamp=%d}",
                    userId, itemId, categoryId, pvCount, hasCartOrFav, latestTimestamp
            );
        }
    }
    // ==================== 辅助方法 ====================

    private static void printBanner() {
        System.out.println("\n==================================================");
        System.out.println("  Flink 实时推荐系统 - 带自定义算子名称版");
        System.out.println("  算子拓扑已命名，便于Web UI监控");
        System.out.println("==================================================");
    }

    private static void printStartupInfo() {
        System.out.println("\n==================================================");
        System.out.println("  流处理任务已启动！");
        System.out.println("  访问 http://localhost:8081 查看Flink Web UI");
        System.out.println("==================================================\n");
    }
}