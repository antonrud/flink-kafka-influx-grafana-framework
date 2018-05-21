package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class FlinkConsumer implements Runnable {

    /* Standart args set:
    --topic
    test
    --bootstrap.servers
    localhost:9092
    --zookeeper.connect
    localhost:2181
    --group.id
    myGroup
     */

    String[] args;

    public FlinkConsumer(String[] args) {
        this.args = args;
    }

    public void consume() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), properties));

        stream.rebalance().map(s -> "Kafka and Flink says: " + s).print();

        env.execute();
    }

    @Override
    public void run() {
        try {
            consume();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
