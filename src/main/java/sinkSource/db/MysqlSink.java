package sinkSource.db;


import org.apache.log4j.LogManager;
import org.apache.spark.sql.ForeachWriter;
import org.slf4j.Logger;
import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;


public class MysqlSink extends ForeachWriter<Object>/*Record*/ {
    private Logger LOGGER = LoggerFactory.getLogger(MysqlSink.class);
    private DruidDataSource druidDataSource = null;
    private Connection connection=null;
    private static final String connUrl = "";
    private static final String driver = "";
    private static final String username = "";
    private static final String password = "";

    @Override
    public boolean open(long partitionId, long version) {

        try {
            connection = druidDataSource.getConnection();
            LOGGER.info("partition【"+partitionId+"】获取mysql链接【"+connection+"】成功");
            return true;
        } catch (SQLException e) {
            LOGGER.error("partition【"+partitionId+"】获取链接失败",e);
            return false;
        }

    }

    @Override
    public void process(Object value) {
        //todo ...
    }

    @Override
    public void close(Throwable errorOrNull) {
      try {
            connection.close();
       } catch (SQLException e) {
            LOGGER.error("关闭资源失败",e);
       }
    }

    // 第三方链接池
    static class Singleton{
         private static MysqlSink instance = new MysqlSink();
         public static MysqlSink getInstance(){
         return instance;
       }
    }

    private MysqlSink(){
         druidDataSource = new DruidDataSource();
         //设置连接参数
         druidDataSource.setUrl(connUrl);
         druidDataSource.setDriverClassName(driver);
         druidDataSource.setUsername(username);
         druidDataSource.setPassword(password);
         // .....
    }

}

