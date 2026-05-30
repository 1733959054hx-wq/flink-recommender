package recommend.optimizer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 数据倾斜优化引擎（P4 贺翔）
 *
 * 两阶段聚合（Local-Global Aggregation）：
 * 第一阶段：加随机前缀打散到多分区局部聚合
 * 第二阶段：去前缀全局聚合
 *
 * 适用于按 itemId 统计商品热度等场景，
 * 防止爆款商品导致单节点负载过高。
 */
public class DataSkewOptimizer {

    private static final int NUM_SLOTS = 10;  // 第一阶段分桶数量
    private static final Random RANDOM = new Random();

    /**
     * 两阶段聚合方法
     * @param input 输入数据流 (itemId, count)
     * @param windowSize 窗口大小（秒）
     * @param slideSize 滑动步长（秒）
     * @return 聚合后的数据流
     */
    public static DataStream<Tuple2<Long, Integer>> twoPhaseAggregate(
            DataStream<Tuple2<Long, Integer>> input,
            long windowSize, long slideSize) {

        // ===== 第一阶段：本地预聚合（添加随机前缀） =====
        DataStream<Tuple2<String, Integer>> stage1 = input
                .map(value -> {
                    int slot = RANDOM.nextInt(NUM_SLOTS);
                    String key = slot + "_" + value.f0;
                    return Tuple2.of(key, value.f1);
                })
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.INT
                ))
                .keyBy(t -> t.f0)
                .window(SlidingProcessingTimeWindows.of(
                        Time.seconds(windowSize),
                        Time.seconds(slideSize)
                ))
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window,
                                      Iterable<Tuple2<String, Integer>> input,
                                      Collector<Tuple2<String, Integer>> out) {
                        int sum = 0;
                        for (Tuple2<String, Integer> t : input) {
                            sum += t.f1;
                        }
                        out.collect(Tuple2.of(key, sum));
                    }
                })
                .name("P4-第一阶段-本地预聚合(添加随机前缀)")
                .uid("p4-stage1-local-agg");

        // ===== 第二阶段：全局聚合（去除前缀） =====
        return stage1
                .map(value -> {
                    String[] parts = value.f0.split("_", 2);
                    Long itemId = Long.parseLong(parts[1]);
                    return Tuple2.of(itemId, value.f1);
                })
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.LONG,
                        org.apache.flink.api.common.typeinfo.Types.INT
                ))
                .keyBy(t -> t.f0)
                .window(SlidingProcessingTimeWindows.of(
                        Time.seconds(windowSize),
                        Time.seconds(slideSize)
                ))
                .apply(new WindowFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Long, TimeWindow>() {
                    @Override
                    public void apply(Long itemId, TimeWindow window,
                                      Iterable<Tuple2<Long, Integer>> input,
                                      Collector<Tuple2<Long, Integer>> out) {
                        int sum = 0;
                        for (Tuple2<Long, Integer> t : input) {
                            sum += t.f1;
                        }
                        out.collect(Tuple2.of(itemId, sum));
                    }
                })
                .name("P4-第二阶段-全局聚合(去除前缀)")
                .uid("p4-stage2-global-agg");
    }
}