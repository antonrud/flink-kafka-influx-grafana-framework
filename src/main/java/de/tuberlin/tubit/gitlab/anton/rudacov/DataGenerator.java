package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

public class DataGenerator implements Runnable {

     /*Standart args set:
    --topic
    test
    --bootstrap.servers
    localhost:9092
*/

    String[] args;
    private String dataPath;

    public DataGenerator(String dataPath) {

        this.dataPath = dataPath;
    }

    private void produce() throws Exception {
        Stream<String> dataStream = getDataStream(dataPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        DataStream<String> stream = env.readTextFile(dataPath);

        stream.addSink(new FlinkKafkaProducer011<String>("localhost:9092","test", new SimpleStringSchema()));

        env.execute();
    }

    private Stream getDataStream(String file) {
        Stream<String> dataStream = null;

        try {
            dataStream = Files.lines(Paths.get(file));
        } catch (IOException e) {
            App.log('f', "Not able to access data file");
        }

        return dataStream;
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
