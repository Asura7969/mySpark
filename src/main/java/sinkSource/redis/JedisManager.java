package sinkSource.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * http://wiki.cheyaoshicorp.com/pages/viewpage.action?pageId=46864116
 *
 * foreachPartition(new ForeachPartitionFunction<T>() {
        @Override
        public void call(Iterator<T> iterator) throws Exception {
            Jedis jedisConn = JedisManager.getInstance().getJedisConn();
            Pipeline pipelined = jedisConn.pipelined();
            try {
                while (iterator.hasNext()){
                    T data = iterator.next();
                    // to do
                }
            } finally {
                pipelined.close();
                jedisConn.close();
            }
         }
    });
 */
public class JedisManager {

    private JedisPool pool = null;
    private static JedisManager jedisManager = new JedisManager();

    private JedisManager(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //REDIS_MAX_IDLE
        jedisPoolConfig.setMaxIdle(10);
        //REDIS_MAX_TOTAL
        jedisPoolConfig.setMaxTotal(20);
        //REDIS_MAX_WAIT
        jedisPoolConfig.setMaxWaitMillis(3000);
        //REDIS_TEST_ON_BORROW
        jedisPoolConfig.setTestOnBorrow(true);

        pool = new JedisPool(jedisPoolConfig,"host",6379/* REDIS_PORT */,1000/* REDIS_MAX_WAIT */, "REDIS_PASSWD");
    }

    public static JedisManager getInstance(){
        return jedisManager;
    }

    public Jedis getJedisConn(){
        return pool.getResource();
    }
} 