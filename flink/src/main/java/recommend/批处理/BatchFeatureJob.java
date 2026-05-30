package recommend.批处理;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchFeatureJob {

    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/recommend_db?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String MYSQL_USER = "flinkpy";
    private static final String MYSQL_PASSWORD = "123456";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple5<Long, Long, Long, String, Long>> behaviorList = readFromDatabase();
        
        DataSet<Tuple5<Long, Long, Long, String, Long>> cleanBehavior = env.fromCollection(behaviorList);

        DataSet<UserFeature> userFeatureDataSet = cleanBehavior
                .groupBy(0)
                .reduceGroup(new UserFeatureReducer());

        DataSet<ItemFeature> itemFeatureDataSet = cleanBehavior
                .groupBy(1)
                .reduceGroup(new ItemFeatureReducer());

        List<ItemFeature> itemFeatures = itemFeatureDataSet.collect();
        double minScore = Double.MAX_VALUE;
        double maxScore = Double.MIN_VALUE;
        for (ItemFeature item : itemFeatures) {
            minScore = Math.min(minScore, item.rawScore);
            maxScore = Math.max(maxScore, item.rawScore);
        }
        final double finalMin = minScore;
        final double finalMax = maxScore;
        
        DataSet<ItemFeature> normalizedItemFeatures = env.fromCollection(itemFeatures)
                .map(item -> {
                    double normalizedScore;
                    if (finalMax - finalMin == 0) {
                        normalizedScore = 0.0;
                    } else {
                        normalizedScore = (item.rawScore - finalMin) / (finalMax - finalMin);
                    }
                    return new ItemFeature(item.itemId, item.categoryId, item.pvCount, item.buyCount, normalizedScore);
                });

        userFeatureDataSet.output(new UserFeatureOutputFormat());
        normalizedItemFeatures.output(new ItemFeatureOutputFormat());

        env.execute("Batch Feature Job from DB");
    }

    private static List<Tuple5<Long, Long, Long, String, Long>> readFromDatabase() throws Exception {
        List<Tuple5<Long, Long, Long, String, Long>> result = new ArrayList<>();
        
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT user_id, item_id, category_id, behavior_type, behavior_time FROM user_behavior")) {
            
            while (rs.next()) {
                long userId = rs.getLong("user_id");
                long itemId = rs.getLong("item_id");
                long categoryId = rs.getLong("category_id");
                String behaviorType = rs.getString("behavior_type");
                long behaviorTime = rs.getLong("behavior_time");
                
                if (behaviorType != null && (behaviorType.equals("pv") || behaviorType.equals("buy") || behaviorType.equals("cart") || behaviorType.equals("fav"))) {
                    result.add(new Tuple5<>(userId, itemId, categoryId, behaviorType, behaviorTime));
                }
            }
        }
        
        return result;
    }

    public static class UserFeatureReducer implements GroupReduceFunction<Tuple5<Long, Long, Long, String, Long>, UserFeature> {
        @Override
        public void reduce(Iterable<Tuple5<Long, Long, Long, String, Long>> behaviors, Collector<UserFeature> collector) {
            long userId = 0;
            int pvCount = 0;
            int buyCount = 0;
            int cartCount = 0;
            int favCount = 0;
            Map<Long, Integer> categoryCount = new HashMap<>();

            for (Tuple5<Long, Long, Long, String, Long> behavior : behaviors) {
                userId = behavior.f0;

                String behaviorType = behavior.f3;
                if ("pv".equals(behaviorType)) {
                    pvCount++;
                } else if ("buy".equals(behaviorType)) {
                    buyCount++;
                } else if ("cart".equals(behaviorType)) {
                    cartCount++;
                } else if ("fav".equals(behaviorType)) {
                    favCount++;
                }

                categoryCount.merge(behavior.f2, 1, Integer::sum);
            }

            double buyRate = pvCount > 0 ? (double) buyCount / pvCount : 0.0;
            double activeScore = calculateActiveScore(pvCount, buyCount, cartCount, favCount);
            long highFreqCategory = findHighFreqCategory(categoryCount);

            collector.collect(new UserFeature(userId, pvCount, buyCount, cartCount, favCount, buyRate, activeScore, highFreqCategory));
        }

        private double calculateActiveScore(int pvCount, int buyCount, int cartCount, int favCount) {
            double score = pvCount * 1.0 + cartCount * 2.0 + favCount * 2.0 + buyCount * 3.0;
            return Math.min(score / 100.0, 100.0);
        }

        private long findHighFreqCategory(Map<Long, Integer> categoryCount) {
            long highFreqCategory = 0;
            int maxCount = 0;
            for (Map.Entry<Long, Integer> entry : categoryCount.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    highFreqCategory = entry.getKey();
                }
            }
            return highFreqCategory;
        }
    }

    public static class ItemFeatureReducer implements GroupReduceFunction<Tuple5<Long, Long, Long, String, Long>, ItemFeature> {
        @Override
        public void reduce(Iterable<Tuple5<Long, Long, Long, String, Long>> behaviors, Collector<ItemFeature> collector) {
            long itemId = 0;
            long categoryId = 0;
            int pvCount = 0;
            int buyCount = 0;

            for (Tuple5<Long, Long, Long, String, Long> behavior : behaviors) {
                itemId = behavior.f1;
                categoryId = behavior.f2;

                String behaviorType = behavior.f3;
                if ("pv".equals(behaviorType)) {
                    pvCount++;
                } else if ("buy".equals(behaviorType)) {
                    buyCount++;
                }
            }

            double rawScore = calculateRawScore(pvCount, buyCount);

            collector.collect(new ItemFeature(itemId, categoryId, pvCount, buyCount, rawScore));
        }

        private double calculateRawScore(int pvCount, int buyCount) {
            return pvCount * 0.3 + buyCount * 0.7;
        }
    }

    public static class UserFeatureOutputFormat implements OutputFormat<UserFeature> {
        private Connection conn;
        private PreparedStatement stmt;

        @Override
        public void configure(Configuration parameters) {}

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                String sql = "REPLACE INTO user_features (" +
                        "user_id, pv_count, buy_count, cart_count, fav_count, buy_rate, active_score, high_freq_category, update_time) " +
                        "VALUES (?,?,?,?,?,?,?,?,?)";
                stmt = conn.prepareStatement(sql);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public void writeRecord(UserFeature feature) throws IOException {
            try {
                stmt.setLong(1, feature.userId);
                stmt.setInt(2, feature.pvCount);
                stmt.setInt(3, feature.buyCount);
                stmt.setInt(4, feature.cartCount);
                stmt.setInt(5, feature.favCount);
                stmt.setDouble(6, feature.buyRate);
                stmt.setDouble(7, feature.activeScore);
                stmt.setLong(8, feature.highFreqCategory);
                stmt.setTimestamp(9, new Timestamp(System.currentTimeMillis()));
                stmt.executeUpdate();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    public static class ItemFeatureOutputFormat implements OutputFormat<ItemFeature> {
        private Connection conn;
        private PreparedStatement stmt;

        @Override
        public void configure(Configuration parameters) {}

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                String sql = "REPLACE INTO item_features (" +
                        "item_id, category_id, pv_count, buy_count, popularity_score, update_time) " +
                        "VALUES (?,?,?,?,?,?)";
                stmt = conn.prepareStatement(sql);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public void writeRecord(ItemFeature feature) throws IOException {
            try {
                stmt.setLong(1, feature.itemId);
                stmt.setLong(2, feature.categoryId);
                stmt.setInt(3, feature.pvCount);
                stmt.setInt(4, feature.buyCount);
                stmt.setDouble(5, feature.popularityScore);
                stmt.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
                stmt.executeUpdate();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    public static class UserFeature {
        public long userId;
        public int pvCount;
        public int buyCount;
        public int cartCount;
        public int favCount;
        public double buyRate;
        public double activeScore;
        public long highFreqCategory;

        public UserFeature(long userId, int pvCount, int buyCount, int cartCount, int favCount,
                           double buyRate, double activeScore, long highFreqCategory) {
            this.userId = userId;
            this.pvCount = pvCount;
            this.buyCount = buyCount;
            this.cartCount = cartCount;
            this.favCount = favCount;
            this.buyRate = buyRate;
            this.activeScore = activeScore;
            this.highFreqCategory = highFreqCategory;
        }
    }

    public static class ItemFeature {
        public long itemId;
        public long categoryId;
        public int pvCount;
        public int buyCount;
        public double popularityScore;
        public double rawScore;

        public ItemFeature(long itemId, long categoryId, int pvCount, int buyCount, double score) {
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.pvCount = pvCount;
            this.buyCount = buyCount;
            this.popularityScore = score;
            this.rawScore = score;
        }
    }
}