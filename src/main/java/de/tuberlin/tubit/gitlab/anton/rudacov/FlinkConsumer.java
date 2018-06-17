package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class FlinkConsumer implements Runnable {


    public FlinkConsumer() {

    }

    public void consume() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", App.KAFKA_BROKER);

        FlinkKafkaConsumer011<String> dataConsumer = new FlinkKafkaConsumer011<String>(App.KAFKA_TOPIC, new SimpleStringSchema(), properties);

        /* Uncomment this if necessary */
        //dataConsumer.setStartFromEarliest();

        DataStream<String> stream = env.addSource(dataConsumer);

        /* InfluxDB sink */
        InfluxDBConfig influxDBConfig = new InfluxDBConfig(InfluxDBConfig.builder(App.INFLUX_URL, App.INFLUX_USER, App.INFLUX_PASS, App.INFLUX_DATABASE));

        stream.map(new InfluxDBMapper())
                .addSink(new InfluxDBSink(influxDBConfig));

        /* Sink to console */
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
