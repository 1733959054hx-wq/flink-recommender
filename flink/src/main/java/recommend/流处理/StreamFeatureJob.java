package recommend.流处理;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class StreamFeatureJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ========== 修改1：Kafka 配置 ==========
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node1.itcast.cn:9092");
        properties.setProperty("group.id", "flink-consumer-group-stream-feature");

        // 从 Kafka 读取数据，替代 Socket
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "user-behavior-topic",           // Kafka topic
                new SimpleStringSchema(),        // 反序列化器
                properties
        );

        // 从最新数据开始消费（对应你的 SQL 中的 scan.startup.mode = 'latest-offset'）
        kafkaConsumer.setStartFromLatest();

        // 添加 Kafka 源
        SingleOutputStreamOperator<String> source = env.addSource(kafkaConsumer);

        // ========== 以下代码保持不变 ==========
        SingleOutputStreamOperator<Tuple5<Long, Long, Long, Integer, Integer>> mapStream = source.map(
                new MapFunction<String, Tuple5<Long, Long, Long, Integer, Integer>>() {
                    @Override
                    public Tuple5<Long, Long, Long, Integer, Integer> map(String line) throws Exception {
                        String[] arr = line.split(",");
                        long userId = Long.parseLong(arr[0].trim());
                        long itemId = Long.parseLong(arr[1].trim());
                        long categoryId = Long.parseLong(arr[2].trim());
                        String behavior = arr[3].trim().toLowerCase();

                        // pv/buy 都算一次浏览，cart/fav 标记为加购收藏
                        int pvCount = ("pv".equals(behavior) || "buy".equals(behavior)) ? 1 : 0;
                        int cartOrFav = ("cart".equals(behavior) || "fav".equals(behavior)) ? 1 : 0;

                        return Tuple5.of(userId, itemId, categoryId, pvCount, cartOrFav);
                    }
                }
        );

        SingleOutputStreamOperator<Tuple5<Long, Long, Long, Integer, Integer>> result = mapStream
                .keyBy(t -> t.f0 + "_" + t.f1)  // 按 userId_itemId 分组
                .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .reduce(new ReduceFunction<Tuple5<Long, Long, Long, Integer, Integer>>() {
                    @Override
                    public Tuple5<Long, Long, Long, Integer, Integer> reduce(
                            Tuple5<Long, Long, Long, Integer, Integer> v1,
                            Tuple5<Long, Long, Long, Integer, Integer> v2) {
                        return Tuple5.of(
                                v1.f0,                  // userId
                                v1.f1,                  // itemId
                                v1.f2,                  // categoryId
                                v1.f3 + v2.f3,          // pvCount 累加
                                v1.f4 | v2.f4           // isCartOrFav 位或（任一为1则结果为1）
                        );
                    }
                });

        SingleOutputStreamOperator<String> outputStream = result.map(
                new MapFunction<Tuple5<Long, Long, Long, Integer, Integer>, String>() {
                    @Override
                    public String map(Tuple5<Long, Long, Long, Integer, Integer> t) {
                        return t.f0 + "," + t.f1 + "," + t.f2 + "," + t.f3 + "," + t.f4 + "," + System.currentTimeMillis();
                    }
                }
        );

        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new org.apache.flink.core.fs.Path("feature"),
                        new org.apache.flink.api.common.serialization.SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(30))
                                .withMaxPartSize(1024 * 1024 * 100)
                                .build())
                .build();

        outputStream.addSink(sink);

        result.print("实时特征 [userId, itemId, categoryId, pvCount5min, isCartOrFav]");

        env.execute("StreamFeatureJob - 5min滑动窗口聚合");
    }
}