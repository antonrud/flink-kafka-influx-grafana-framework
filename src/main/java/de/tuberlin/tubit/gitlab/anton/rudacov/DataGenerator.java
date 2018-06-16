package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

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

        //Extract, assign and cut timestamps from data
        stream.assignTimestampsAndWatermarks(new TimestampExtractor())
                .addSink(new FlinkKafkaProducer011<String>("217.163.23.24:9092", "morse", new SimpleStringSchema()));



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
