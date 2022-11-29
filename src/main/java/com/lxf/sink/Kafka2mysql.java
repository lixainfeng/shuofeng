package com.lxf.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class Kafka2mysql {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Readkafka(env);
        env.execute();
    }
    public static void Readkafka(StreamExecutionEnvironment env){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "110.40.173.142:9092");
        properties.setProperty("group.id", "com.lxf.test.test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<String>("test",new SimpleStringSchema(),properties));
        stream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(new Tuple2<>(s,1));
                }
            }
        })
                .print();
    }
}
