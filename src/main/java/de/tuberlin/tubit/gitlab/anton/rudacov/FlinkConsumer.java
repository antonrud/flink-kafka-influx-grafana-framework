package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

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
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer082<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));

        messageStream.rebalance().map(s -> "Kafka and Flink says: " + s).print();

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
