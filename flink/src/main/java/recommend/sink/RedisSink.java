package recommend.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import recommend.StreamRecommendJob.Recommendation;

public class RedisSink extends RichSinkFunction<Recommendation> {

    private static final String REDIS_HOST = "192.168.88.8";
    private static final int REDIS_PORT = 6379;
//    private static final String REDIS_PASSWORD = "123456";
    private static final String REDIS_PASSWORD = "";
    private static final int ZSET_MAX_SIZE = 50;
    private static final int TTL_SECONDS = 86400;

    private transient JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(2);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 2000);
        System.out.println("[RedisSink] JedisPool连接池已建立: " + REDIS_HOST + ":" + REDIS_PORT);
    }

    @Override
    public void invoke(Recommendation value, Context context) throws Exception {
        if (value == null) return;

        try (Jedis jedis = jedisPool.getResource()) {
            String key = "rec:" + value.userId;
            String metaKey = "rec_meta:" + value.userId;

            // ★ member 只存 itemId（保证唯一性，zadd更新score）
            jedis.zadd(key, value.finalScore.doubleValue(), String.valueOf(value.itemId));

            // ★ predictScore 单独存到 hash 中
            jedis.hset(metaKey, String.valueOf(value.itemId), value.predictScore.toPlainString());

            // ★ 设置 hash 的过期时间（与 sorted set 同步）
            jedis.expire(metaKey, TTL_SECONDS);

            // 限制 sorted set 大小
            jedis.zremrangeByRank(key, 0, -(ZSET_MAX_SIZE + 1));
            jedis.expire(key, TTL_SECONDS);
        }
    }



    @Override
    public void close() throws Exception {
        if (jedisPool != null) {
            jedisPool.close();
            System.out.println("[RedisSink] JedisPool连接池已关闭");
        }
        super.close();
    }
}