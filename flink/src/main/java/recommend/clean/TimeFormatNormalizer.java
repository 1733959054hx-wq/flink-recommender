package recommend.clean;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * 时间格式归一化算子（P5 林俊贤）
 *
 * 功能：
 * 1. 统一将时间格式转为秒级时间戳（如果数据中是毫秒级则除以1000）
 * 2. 兼容不同长度的时间戳格式
 *
 * 使用方式：
 *   DataStream<String> normalizedStream = rawStream.map(new TimeFormatNormalizer());
 */
public class TimeFormatNormalizer implements MapFunction<String, String> {

    // 毫秒级时间戳下界（对应 2016-01-01 00:00:00 UTC）
    private static final long MILLIS_LOWER_BOUND = 1451606400000L;

    @Override
    public String map(String value) throws Exception {
        if (value == null || value.trim().isEmpty()) {
            return value;
        }

        String trimmed = value.trim();
        String[] fields = trimmed.split(",");

        // 只处理格式正确的5字段数据
        if (fields.length != 5) {
            return value;
        }

        try {
            // 第5个字段是时间戳
            String timestampStr = fields[4].trim();
            long timestamp = Long.parseLong(timestampStr);

            // 如果是毫秒级时间戳（大于2116-01-01对应的毫秒值），则除以1000转为秒级
            // 判断逻辑：> 20亿(秒) 且 > 毫秒下界，说明是毫秒
            if (timestamp > 2000000000L && timestamp >= MILLIS_LOWER_BOUND) {
                timestamp = timestamp / 1000;
                fields[4] = String.valueOf(timestamp);
                return String.join(",", fields);
            }
        } catch (NumberFormatException ignored) {
            // 非数字时间戳，不处理
        }

        return value;
    }
}
