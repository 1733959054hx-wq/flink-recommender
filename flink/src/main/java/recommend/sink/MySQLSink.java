package recommend.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 通用 MySQL Sink（P6 范忠睿）
 *
 * 将 Flink 流处理产生的推荐结果写入 MySQL recommendations 表。
 * 支持：
 * - UPSERT（REPLACE INTO）语义
 * - 批量写入优化
 * - 异常重试
 */
public class MySQLSink extends RichSinkFunction<MySQLSink.RecommendationResult> {

    // ==================== 配置 ====================
    private static final String JDBC_URL = "jdbc:mysql://192.168.88.8:3306/recommend_db?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true";
    private static final String JDBC_USER = "flinkpy";
    private static final String JDBC_PASSWORD = "123456";

    private transient Connection conn;
    private transient PreparedStatement stmt;
    private int batchCount = 0;
    private static final int BATCH_SIZE = 50;

    /**
     * 推荐结果数据模型（与 StreamRecommendJob.Recommendation 对齐）
     */
    public static class RecommendationResult {
        public long userId;
        public long itemId;
        public BigDecimal predictScore;
        public BigDecimal popularityScore;
        public BigDecimal finalScore;
        public int rankNo;

        public RecommendationResult(long userId, long itemId, BigDecimal predictScore,
                                    BigDecimal popularityScore, BigDecimal finalScore, int rankNo) {
            this.userId = userId;
            this.itemId = itemId;
            this.predictScore = predictScore;
            this.popularityScore = popularityScore;
            this.finalScore = finalScore;
            this.rankNo = rankNo;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);

        String sql = "INSERT INTO recommendations " +
                "(user_id, item_id, predict_score, popularity_score, final_score, rank_no, recommend_type, create_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, 'realtime', NOW()) " +
                "ON DUPLICATE KEY UPDATE " +
                "predict_score=VALUES(predict_score), popularity_score=VALUES(popularity_score), " +
                "final_score=VALUES(final_score), rank_no=VALUES(rank_no)";   // ← 更新时不再改变 create_time
        stmt = conn.prepareStatement(sql);
        System.out.println("[MySQLSink] 数据库连接已建立");
    }

    @Override
    public void invoke(RecommendationResult value, Context context) throws Exception {
        stmt.setLong(1, value.userId);
        stmt.setLong(2, value.itemId);
        stmt.setBigDecimal(3, value.predictScore);
        stmt.setBigDecimal(4, value.popularityScore);
        stmt.setBigDecimal(5, value.finalScore);
        stmt.setInt(6, value.rankNo);
        stmt.addBatch();
        batchCount++;

        // 批量提交
        if (batchCount >= BATCH_SIZE) {
            stmt.executeBatch();
            batchCount = 0;
        }
    }

    @Override
    public void close() throws Exception {
        // 提交剩余批次
        if (stmt != null && batchCount > 0) {
            stmt.executeBatch();
        }
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
        System.out.println("[MySQLSink] 数据库连接已关闭");
    }
}
