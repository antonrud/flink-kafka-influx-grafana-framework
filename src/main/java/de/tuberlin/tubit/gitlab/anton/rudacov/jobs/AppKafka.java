package de.tuberlin.tubit.gitlab.anton.rudacov.jobs;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.DataPointSerializationSchema;
import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.functions.SensorDataWatermarkAssigner;
import de.tuberlin.tubit.gitlab.anton.rudacov.sinks.InfluxDBSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class AppKafka {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Common Flink settings
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
        env.enableCheckpointing(1000);
        env.setParallelism(1);
        // Be careful with this
        //env.disableOperatorChaining();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Data Processor
        // Kafka consumer properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "217.163.23.24:9092");
        kafkaProperties.setProperty("group.id", "morse");

        // Create Kafka Consumer
        FlinkKafkaConsumer011<KeyedDataPoint<Integer>> kafkaConsumer =
                new FlinkKafkaConsumer011<>("morse", new DataPointSerializationSchema(), kafkaProperties);

        // Add it as a source
        SingleOutputStreamOperator<KeyedDataPoint<Integer>> morseStream = env.addSource(kafkaConsumer);

        morseStream = morseStream.assignTimestampsAndWatermarks(new SensorDataWatermarkAssigner());

        // Write this sensor stream out to InfluxDB
        morseStream
                .addSink(new InfluxDBSink<>("sensors"));

        // Compute a windowed sum over this data and write that to InfluxDB as well.
        morseStream
                .keyBy("key")
                .timeWindow(Time.seconds(1))
                .sum("value")
                .addSink(new InfluxDBSink<>("summedSensors"));

        // execute program
        env.execute("OSCON Example");
    }
}
