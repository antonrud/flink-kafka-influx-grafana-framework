package de.tuberlin.tubit.gitlab.anton.rudacov.jobs;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.DataPointSerializationSchema;
import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.sinks.InfluxDBSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class KafkaConsumer implements Runnable {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    public KafkaConsumer() {

    }

    private void consume() throws Exception {

        // Common Flink settings
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
        env.enableCheckpointing(1000);
        env.setParallelism(1);
        // Be careful with this
        //env.disableOperatorChaining();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Kafka consumer properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "217.163.23.24:9092");
        kafkaProperties.setProperty("group.id", "morse");

        // Create Kafka Consumer
        FlinkKafkaConsumer011<KeyedDataPoint<Integer>> kafkaConsumer =
                new FlinkKafkaConsumer011<>("morse", new DataPointSerializationSchema(), kafkaProperties);

        // Add it as a source
        SingleOutputStreamOperator<KeyedDataPoint<Integer>> morseKafkaStream = env.addSource(kafkaConsumer);

        // Write this stream out to InfluxDB
        morseKafkaStream
                .addSink(new InfluxDBSink<>("kafkaMorse"));

        //TODO Replace with Morse interpretation logic and sink to Influx as well
        // Compute a windowed sum over this data and write that to InfluxDB as well.
        /* morseKafkaStream
                .keyBy("key")
                .timeWindow(Time.seconds(1))
                .sum("value")
                .addSink(new InfluxDBSink<>("summedSensors")); */

        // Execute Flink
        env.execute("Morse Kafka");
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
