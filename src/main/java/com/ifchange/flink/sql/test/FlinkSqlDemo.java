package com.ifchange.flink.sql.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * flink-sql
 *   kafka read
 *   mysql write
 */
public class FlinkSqlDemo {

    public static final String KAFKA_TABLE_SOURCE_DDL = "CREATE TABLE user_behavior (\n"
        + "    user_id BIGINT,\n"
        + "    item_id BIGINT,\n"
        + "    category_id BIGINT,\n"
        + "    behavior STRING,\n"
        + "    ts TIMESTAMP(3)\n"
        + ") WITH (\n"
        + "    'connector.type' = 'kafka',  -- 指定连接类型是kafka\n"
        + "    'connector.version' = '0.10',  -- 与我们之前Docker安装的kafka版本要一致\n"
        + "    'connector.topic' = 'flink-sql', -- 之前创建的topic \n"
        + "    'connector.properties.group.id' = 'flink-test-1', -- 消费者组，相关概念可自行百度\n"
        + "    'connector.startup-mode' = 'earliest-offset',  --指定从最早消费\n"
        + "    'connector.properties.zookeeper.connect' = '192.168.1.200:2181',  -- zk地址\n"
        + "    'connector.properties.bootstrap.servers' = '192.168.1.200:9092,192.168.1.107:9092',  -- broker地址\n"
        + "    'format.type' = 'json' , -- json格式，和topic中的消息格式保持一致\n"
        + "    'update-mode' = 'append'\n"
        + ")";

    public static final String MYSQL_TABLE_SINK_DDL = "CREATE TABLE `user_behavior_mysql` (\n"
        + "  `user_id` bigint  ,\n"
        + "  `item_id` bigint  ,\n"
        + "  `behavior` varchar  ,\n"
        + "  `category_id` bigint  ,\n"
        + "  `ts` timestamp(3)   \n"
        + ")WITH (\n"
        + "  'connector.type' = 'jdbc', -- 连接方式\n"
        + "  'connector.url' = 'jdbc:mysql://192.168.1.201:3310/test', -- jdbc的url\n"
        + "  'connector.table' = 'user_behavior',  -- 表名\n"
        + "  'connector.driver' = 'com.mysql.jdbc.Driver', -- 驱动名字，可以不填，会自动从上面的jdbc url解析 \n"
        + "  'connector.username' = 'devuser', -- 顾名思义 用户名\n"
        + "  'connector.password' = 'devuser' , -- 密码\n"
        + "  'connector.write.flush.max-rows' = '100', -- 意思是攒满多少条才触发写入 \n"
        + "  'connector.write.flush.interval' = '2s' -- 意思是攒满多少秒才触发写入；这2个参数，无论数据满足哪个条件，就会触发写入\n"
        + ")";

    public static void main(String[] args) throws Exception {
        System.out.println(KAFKA_TABLE_SOURCE_DDL);
        //构建StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //构建EnvironmentSettings 并指定Blink Planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //构建StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);
        //通过DDL，注册kafka数据源表
        tEnv.executeSql(KAFKA_TABLE_SOURCE_DDL);
        //通过DDL，注册mysql数据结果表
        tEnv.executeSql(MYSQL_TABLE_SINK_DDL);
        //将从kafka中查到的数据，插入mysql中
        tEnv.executeSql("insert into user_behavior_mysql select user_id,item_id,behavior,category_id,ts from user_behavior");
        //任务启动，这行必不可少！
        env.execute("test");
    }
}
