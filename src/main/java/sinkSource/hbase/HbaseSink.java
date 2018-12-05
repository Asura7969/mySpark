package sinkSource.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.sql.ForeachWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HbaseSink extends ForeachWriter<Object>/*Record*/ {
    private static String resourcePath = "";
    private static Logger LOGGER = LoggerFactory.getLogger(HbaseSink.class);
    private Connection connection = null;
    private Table htable = null;
    private String tableName = "tableName";

    @Override
    public boolean open(long partitionId, long version) {
        try {
            htable = connection.getTable(TableName.valueOf(tableName));
            LOGGER.info("partition【"+partitionId+"】链接htable【"+htable+"】成功");
        } catch (IOException e) {
            LOGGER.error("partition【"+partitionId+"】链接htable失败");
        }
        return true;
    }

    @Override
    public void process(Object value) {
        //TODO
    }

    @Override
    public void close(Throwable errorOrNull) {
        try {
            htable.close();
        } catch (IOException e) {
            LOGGER.error("关闭资源失败",e);
        }
    }


    static class Singleton{
       private static HbaseSink hbaseSink = new HbaseSink();
       public static HbaseSink getInstance(){
         return hbaseSink;
       }
    }

    private HbaseSink(){
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.addResource(resourcePath);
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            LOGGER.error("获取链接失败",e);
        }
    }
}