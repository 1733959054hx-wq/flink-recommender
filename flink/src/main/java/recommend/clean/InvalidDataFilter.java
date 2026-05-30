package recommend.clean;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

/**
 * 数据清洗算子（P5 林俊贤）
 *
 * 功能：
 * 1. 过滤缺失/空值记录
 * 2. 过滤非数字字段（userId/itemId/categoryId/timestamp）
 * 3. 过滤无效行为类型（仅允许 pv/buy/cart/fav）
 * 4. 过滤异常时间戳（超出合理范围）
 * 5. 统一时间格式（秒级时间戳）
 *
 * 使用方式：
 *   DataStream<String> cleanedStream = rawStream.filter(new InvalidDataFilter());
 */
public class InvalidDataFilter extends RichFilterFunction<String> {

    private static final long TIMESTAMP_LOWER_BOUND = 1451606400L;  // 2016-01-01
    private static final long TIMESTAMP_UPPER_BOUND = 1893456000L;  // 2030-01-01

    private transient long totalCount;
    private transient int filteredCount;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        totalCount = 0;
        filteredCount = 0;
    }

    @Override
    public boolean filter(String value) throws Exception {
        totalCount++;

        // 1. 跳过空行
        if (value == null || value.trim().isEmpty()) {
            filteredCount++;
            return false;
        }

        String trimmed = value.trim();
        String[] fields = trimmed.split(",");

        // 2. 检查字段数量
        if (fields.length != 5) {
            System.err.println("[CleanFilter] 字段数错误: 期望5个，实际" + fields.length + " -> " + trimmed);
            filteredCount++;
            return false;
        }

        try {
            // 3. 检查 userId（正数）
            long userId = Long.parseLong(fields[0].trim());
            if (userId <= 0) {
                System.err.println("[CleanFilter] userId <= 0: " + userId);
                filteredCount++;
                return false;
            }

            // 4. 检查 itemId（正数）
            long itemId = Long.parseLong(fields[1].trim());
            if (itemId <= 0) {
                System.err.println("[CleanFilter] itemId <= 0: " + itemId);
                filteredCount++;
                return false;
            }

            // 5. 检查 categoryId（正数）
            long categoryId = Long.parseLong(fields[2].trim());
            if (categoryId <= 0) {
                System.err.println("[CleanFilter] categoryId <= 0: " + categoryId);
                filteredCount++;
                return false;
            }

            // 6. 检查 behaviorType（仅允许 pv/buy/cart/fav）
            String behaviorType = fields[3].trim().toLowerCase();
            if (!"pv".equals(behaviorType) && !"buy".equals(behaviorType)
                    && !"cart".equals(behaviorType) && !"fav".equals(behaviorType)) {
                System.err.println("[CleanFilter] 无效行为类型: " + behaviorType);
                filteredCount++;
                return false;
            }

            // 7. 检查时间戳（合理范围内）
            long timestamp = Long.parseLong(fields[4].trim());
            if (timestamp < TIMESTAMP_LOWER_BOUND || timestamp > TIMESTAMP_UPPER_BOUND) {
                System.err.println("[CleanFilter] 异常时间戳: " + timestamp + " (合理范围: "
                        + TIMESTAMP_LOWER_BOUND + " ~ " + TIMESTAMP_UPPER_BOUND + ")");
                filteredCount++;
                return false;
            }

            return true;  // 所有检查通过

        } catch (NumberFormatException e) {
            System.err.println("[CleanFilter] 非数字字段: " + trimmed);
            filteredCount++;
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        double filteredPercent = totalCount > 0
                ? (filteredCount * 100.0 / totalCount) : 0;
        System.out.println("[CleanFilter] 数据清洗完成: 总共 " + totalCount
                + " 条, 过滤 " + filteredCount + " 条 (" + String.format("%.1f", filteredPercent) + "%)");
    }
}
