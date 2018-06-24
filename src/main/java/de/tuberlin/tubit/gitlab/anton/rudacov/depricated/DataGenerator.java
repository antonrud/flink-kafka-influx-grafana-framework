package de.tuberlin.tubit.gitlab.anton.rudacov.depricated;

import de.tuberlin.tubit.gitlab.anton.rudacov.App;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;


public class DataGenerator implements Runnable {

    private String dataPath;

    public DataGenerator(String dataPath) {
        this.dataPath = dataPath;
    }

    private void produce() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.readTextFile(dataPath);
        stream.addSink(new FlinkKafkaProducer011<>(App.KAFKA_BROKER, App.KAFKA_TOPIC, new SimpleStringSchema()));
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
