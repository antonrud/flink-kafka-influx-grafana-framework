package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Properties;

public class FlinkConsumer implements Runnable {

    private String[] args;

    public FlinkConsumer(String[] args) {

        this.args = args;
    }

    public void consume() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", App.KAFKA_BROKER);

        FlinkKafkaConsumer011<String> dataConsumer = new FlinkKafkaConsumer011<String>(App.KAFKA_TOPIC, new SimpleStringSchema(), properties);

        //dataConsumer.setStartFromEarliest();

        DataStream<String> stream = env.addSource(dataConsumer);

/*
        TODO: InfluxDB sink

        DataStream<InfluxDBPoint> dataStream = stream.map(
                new RichMapFunction<String, InfluxDBPoint>() {


                    @Override
                    public InfluxDBPoint map(String s) throws Exception {

                        long timestamp = App.TIMESTAMP_FORMAT.parse(s.split(";")[0]).getTime();
                        String measurement = s.split(";")[1];

                        HashMap<String, String> tags = new HashMap<>();
                        tags.put("host", input[1]);
                        tags.put("region", "region#" + String.valueOf(input[1].hashCode() % 20));

                        HashMap<String, Object> fields = new HashMap<>();
                        fields.put("value1", input[1].hashCode() % 100);
                        fields.put("value2", input[1].hashCode() % 50);

                        return new InfluxDBPoint(measurement, timestamp, tags, fields);
                    }
                }
        );

        InfluxDBConfig influxDBConfig = InfluxDBConfig.builder("217.163.23.24:8086", "admin", "DBPROgruppe3", "morse");
        dataStream.addSink(new InfluxDBSink(influxDBConfig));

*/

        stream/*.rebalance() this causes strange reorder of messages*/
                .map(x -> x.split(";")[1])
                .map(x -> Integer.parseInt(x.trim()) > 7500 ? 0 : 1)
                .map(x -> "Received state: " + x)
                .print();

        env.execute();

    }

    @Override
    public void run() {
        App.log('i', "Consumer starting...");

        try {
            consume();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
