package sinkSource.redis;

import org.apache.spark.sql.ForeachWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisSink extends ForeachWriter<Object>/*Record*/ {
    private Logger LOGGER = LoggerFactory.getLogger(RedisSink.class);
    private JedisPool jedisPool = null;
    private Jedis jedis = null;
    private static final int maxTotal = 10;
    private static final int maxWaitTimeMill = 10*1000;
    private static final int maxIdle = 5;
    private static final boolean testOnBorrow = true;

    private static final String host = "127.0.0.1";
    private static final int port = 6379;

    @Override
    public boolean open(long partitionId, long version) {
        try {
             jedis = jedisPool.getResource();
             LOGGER.info("partition【"+partitionId+"】获取redis链接【"+jedis+"】成功");
             return true;
        } catch (Exception e) {
             LOGGER.error("partition【"+partitionId+"】获取链接失败",e);
             return false;
        }
    }

    @Override
    public void process(Object/*Record*/ value) {
        // jedis to do
    }

    @Override
    public void close(Throwable errorOrNull) {
        try {
            jedis.close();
        } catch (Exception e) {
            LOGGER.error("关闭资源失败",e);
        }
    }

    static class Singleton{
       private static RedisSink redisSink = new RedisSink();
       public static RedisSink getInstance(){
         return redisSink;
       }
    }

    // jedis pool
    private RedisSink (){
       JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
       jedisPoolConfig.setMaxIdle(maxIdle);
       jedisPoolConfig.setMaxTotal(maxTotal);
       jedisPoolConfig.setMaxWaitMillis(maxWaitTimeMill);
       jedisPoolConfig.setTestOnBorrow(testOnBorrow);
       jedisPool = new JedisPool(jedisPoolConfig,host,port);
    }

}