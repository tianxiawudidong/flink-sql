package com.ifchange.flink.sql.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

public class KafkaPipelineTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2 定义到kafka的连接，创建输入表
        tableEnv.connect(new Kafka()
            .version("0.11") // 定义版本
            .topic("sensor") // 定义主题
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")
        ).withFormat(new Json())
            .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.DOUBLE())
                .field("pt", DataTypes.TIMESTAMP(3))
                .proctime()
            ).createTemporaryTable("kafkaInputTable");

        // 3. 表的查询转换
        Table sensorTable = tableEnv.from("kafkaInputTable");
        // 3.1 简单查询转换
        //.groupBy($("a"))
        //        .select($("a"), $("b").count().as("cnt"));

        Table resultTable = sensorTable
            .select($("id"))
            //tab.filter($("name").isEqual("Fred"));
            //tab.filter($"name" === "Fred")
            .filter($("id").isEqual("'sensor_1'"));

        // 3.2 聚合转换
        Table aggResultTable = sensorTable
            //tab.groupBy($("key")).select($("key"), $("value").avg());
            .groupBy($("id"))
            .select($("id"), $("id").count());

        tableEnv.connect(new Kafka()
            .version("0.11") // 定义版本
            .topic("sinkTest") // 定义主题
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")
        ).withFormat(new Json())
            .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.DOUBLE())
                .field("pt", DataTypes.TIMESTAMP(3))
            ).createTemporaryTable("kafkaOutputTable");

        resultTable.executeInsert("kafkaOutputTable");
        aggResultTable.executeInsert("kafkaOutputTable");

        env.execute("kafka pipeline test job");
    }
}
