package de.tuberlin.tubit.gitlab.anton.rudacov;

import de.tuberlin.tubit.gitlab.anton.rudacov.influxdb.InfluxDBConfig;
import de.tuberlin.tubit.gitlab.anton.rudacov.influxdb.InfluxDBPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.influxdb.InfluxDBSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerRecord;
/*import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
*/
import java.awt.*;
import java.util.Arrays;
import java.util.Properties;

public class FlinkConsumer implements Runnable {
    String[] args;
    Properties properties;

    public FlinkConsumer(String[] args) {
        this.args = args;
    }

    public void consume() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        FlinkKafkaConsumer011<String> dataConsumer = new FlinkKafkaConsumer011<>(App.KAFKA_TOPIC, new SimpleStringSchema(), this.properties);
        //dataConsumer.setStartFromEarliest();

        DataStream<String> stream = env.addSource(dataConsumer).assignTimestampsAndWatermarks(new CustomTimestampExtractor());

        /* InfluxDB sink */
        InfluxDBConfig influxDBConfig = new InfluxDBConfig(InfluxDBConfig.builder(App.dbHost, App.dbUser, App.dbPass, App.dbName));
        //stream.map(new InfluxDBMapper())
          //      .addSink(new InfluxDBSink(influxDBConfig));

//        stream.print();
            //.rebalance()
/*            .map(
                new MapFunction<String, Tuple3<String, String, Long>>() {
                    private static final long serialVersionUID = 34_2016_10_19_001L;
                    @Override
                    public Tuple3<String, String, Long> map(String value) throws Exception {
                        String[] splits = value.split("\\|");
                        return new Tuple3<String, String, Long>(
                            splits[0],
                            splits[1],
                            Long.parseLong(splits[2])
                        );
                    }
                }
            )*/
            //.print();
//
//        stream/*.rebalance() this causes strange reorder of messages*/
//                .map(x -> x.split(";")[1])
//                .map(x -> Integer.parseInt(x.trim()) > 7500 ? 0 : 1)
//                .map(x -> "Received state: " + x)
//                .print();
//
        env.execute();

    }
    public void consumeWithTime() throws Exception {
        /*KafkaConsumer<String, String> consumer = new KafkaConsumer(this.properties);
        consumer.subscribe(Arrays.asList(App.KAFKA_TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s, time = %d%n", record.offset(), record.key(), record.value(), record.timestamp());
        }*/
/*

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer011<String> dataConsumer = new FlinkKafkaConsumer011<>(App.KAFKA_TOPIC, new SimpleStringSchema(), this.properties);

        dataConsumer.assignTimestampsAndWatermarks(new CustomTimestampExtractor());

        DataStream<String> stream = env.addSource(dataConsumer);
        DataStream<Tuple2<String,Long>> streamTuples = stream.flatMap(new Json2Tuple());

        stream.rebalance().map(s -> "I've got a line: " + s).print();

        env.execute();
*/
    }

    @Override
    public void run() {
        try {
            this.properties = new Properties();
            this.properties.put("bootstrap.servers", App.KAFKA_BROKER);
            this.properties.put("auto.offset.reset", "earliest");
            this.properties.put("group.id", App.KAFKA_TOPIC);
            this.properties.put("enable_auto_commit", "true");
            this.properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            this.properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consume();
            //consumeWithTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
