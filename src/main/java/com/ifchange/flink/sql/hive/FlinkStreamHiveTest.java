package com.ifchange.flink.sql.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkStreamHiveTest {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkStreamHiveTest.class);

    private static final String kafkaSql = "CREATE TABLE kafka_table (\n" +
        "  created_at STRING,\n" +
        "  tid SMALLINT,\n" +
        "  uid BIGINT,\n" +
        "  updated_at STRING,\n" +
        "  url STRING,\n" +
        "  ip STRING\n" +
        ") \n" +
        " WITH (\n" +
        "  'connector.type' = 'kafka',\n" +
        "  'connector.version' = '0.11',\n" +
        "  'connector.topic' = 'icdc_log_test',\n" +
        "  'connector.startup-mode' = 'latest-offset',\n" +
        "  'connector.properties.zookeeper.connect' = '192.168.9.129:2181,192.168.9.130:2181,192.168.9.131:2181,192.168.9.132:2181,192.168.9.133:2181',\n" +
        "  'connector.properties.bootstrap.servers' = '192.168.9.129:9092,192.168.9.130:9092,192.168.9.131:9092,192.168.9.132:9092,192.168.9.133:9092',\n" +
        "  'connector.properties.group.id' = 'flink_group_11',\n" +
        "  'format.type' = 'csv',\n" +
        "  'format.derive-schema' = 'true'\n" +
        ")";

    private static final String hiveSql = "CREATE TABLE active_users (\n" +
        "  created_at STRING,\n" +
        "  tid SMALLINT,\n" +
        "  uid BIGINT,\n" +
        "  updated_at STRING,\n" +
        "  url STRING,\n" +
        "  ip STRING\n" +
        ") PARTITIONED BY (dt STRING, hr STRING, minut STRING) STORED AS parquet TBLPROPERTIES (\n" +
        "  'partition.time-extractor.timestamp-pattern'='$dt $hr:minut:00',\n" +
        "  'sink.partition-commit.trigger'='process-time',\n" +
//        "  'sink.partition-commit.delay'='5 min',\n" +
        "  'sink.partition-commit.policy.kind'='metastore,success-file,custom'\n" +
        //自定义合并策略
        "'sink.partition-commit.policy.class'='com.ifchange.flink.sql.hive.ParquetFileMergingCommitPolicy'"+
        ")";

    public static void main(String[] args) throws Exception {
        //构建StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getCheckpointConfig().setCheckpointInterval(1000 * 60);
        //构建EnvironmentSettings 并指定Blink Planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //构建StreamTableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "/opt/userhome/hadoop/apache-hive-2.3.5/conf"; // a local path
        String version = "2.3.5";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);
        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");

//        tableEnv.createTemporarySystemFunction();

        try {
            TableResult tableResult = tableEnv.executeSql(kafkaSql);
            String name1 = tableResult.getResultKind().name();
            System.out.println(name1);
        } catch (Exception e) {
            LOG.error("execute kafka sql：{} parse error：{}", kafkaSql, e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        try {
            TableResult tableResult = tableEnv.executeSql(hiveSql);
            String name1 = tableResult.getResultKind().name();
            System.out.println(name1);
        } catch (Exception e) {
            LOG.error("execute hive sql：{} parse error：{}", hiveSql, e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        //-- streaming sql, insert into hive table
        String sql = "INSERT INTO active_users \n" +
            "SELECT created_at, tid, uid, updated_at, url, ip,\n" +
//            " TO_DATE(created_at, 'yyyy-MM-dd'),\n " +
//            " HOUR(TO_TIMESTAMP(created_at, 'yyyy-MM-dd HH:mm:ss')),\n" +
//            " MINUTE(TO_TIMESTAMP(created_at, 'yyyy-MM-dd HH:mm:ss')) \n" +
            "  DATE_FORMAT(created_at,'yyyy-MM-dd'), \n" +
            "  DATE_FORMAT(created_at,'HH'), \n" +
            "  DATE_FORMAT(created_at,'mm') \n" +
            "FROM kafka_table";
        LOG.info("{}", sql);

        tableEnv.executeSql(sql);

//        env.execute("flink-stream-hive-test");

    }
}
