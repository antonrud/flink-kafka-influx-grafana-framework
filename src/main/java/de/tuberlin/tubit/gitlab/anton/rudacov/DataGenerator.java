package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;

import java.util.Arrays;
import java.util.Properties;

public class DataGenerator implements Runnable {

    private String dataPath;

    public DataGenerator(String dataPath) {
        this.dataPath = dataPath;
    }

    private void produce() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //To work with event time, streaming programs need to set the time characteristic.
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //Properties properties = new Properties();
        //properties.setProperty("bootstrap.serveStartFromEarliesrs", "localhost:9092");

        DataStream<String> stream = env.readTextFile(dataPath);

        /* Drop previous measurements in InfluxDB */
        InfluxDB influxDB = InfluxDBFactory.connect(App.INFLUX_URL, App.INFLUX_USER, App.INFLUX_PASS);
        influxDB.setDatabase(App.INFLUX_DATABASE);
        Query query = new Query("DROP MEASUREMENT morseMeasurement", App.INFLUX_DATABASE);
        influxDB.query(query);
        influxDB.close();

        /* InfluxDB sink */
        DataStream<InfluxDBPoint> dataStream = stream.map(new InfluxDBMapper());
        InfluxDBConfig influxDBConfig = new InfluxDBConfig(InfluxDBConfig.builder(App.INFLUX_URL, App.INFLUX_USER, App.INFLUX_PASS, App.INFLUX_DATABASE));
        dataStream.addSink(new InfluxDBSink(influxDBConfig));

        //Extract, assign and cut timestamps from data
        stream.assignTimestampsAndWatermarks(new TimestampExtractor())
                /* .map(x -> x.split(";")[1]) TODO Add this after timestamp extraction is working on consumer side */
                .addSink(new FlinkKafkaProducer011<String>(App.KAFKA_BROKER, App.KAFKA_TOPIC, new SimpleStringSchema()));

        env.execute();
    }

    @Override
    public void run() {
        App.log('i', "Generator starting...");

        try {
            produce();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
