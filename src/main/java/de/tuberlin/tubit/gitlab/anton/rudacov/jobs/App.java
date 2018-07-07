package de.tuberlin.tubit.gitlab.anton.rudacov.jobs;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.DataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.data.DataPointSerializationSchema;
import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.functions.AssignKeyFunction;
import de.tuberlin.tubit.gitlab.anton.rudacov.functions.MorseWindowFunction;
import de.tuberlin.tubit.gitlab.anton.rudacov.functions.ResistanceFunction;
import de.tuberlin.tubit.gitlab.anton.rudacov.sinks.InfluxDBSink;
import de.tuberlin.tubit.gitlab.anton.rudacov.sources.TimestampSource;
import de.tuberlin.tubit.gitlab.anton.rudacov.tools.MeasurementDrop;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class App {

    public static final String KAFKA_BROKER = "217.163.23.24:9092";
    public static final String KAFKA_TOPIC = "morse";

    public static void main(String[] args) throws Exception {

        // Start Kafka consumer
        new Thread(new KafkaConsumer()).start();

        // Drop previous measurements
        MeasurementDrop.drop("morse");
        MeasurementDrop.drop("kafkaMorse");

        // set up the execution environment
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable fault-tolerance for state
        env.enableCheckpointing(1000);

        // Enable Event Time processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Generate stream with morse data
        DataStream<KeyedDataPoint<Integer>> morseStream = generateSensorData(env);

        // Writes sensor stream out to InfluxDB
        morseStream
                .addSink(new InfluxDBSink<>("morse"));

        //Sink to Kafka
        morseStream
                .addSink(new FlinkKafkaProducer011<>(KAFKA_BROKER, KAFKA_TOPIC, new DataPointSerializationSchema()));


        //TODO Replace with Morse interpretation logic and sink to Influx as well
        // Compute a windowed sum over this data and write that to InfluxDB as well.
        morseStream
                .filter(keyedDataPoint -> keyedDataPoint.getValue() < 7500)
                .windowAll(EventTimeSessionWindows.withGap(Time.seconds(2)))
                .apply(new MorseWindowFunction());

        // Execute Flink
        env.execute("Morse code");
    }

    private static DataStream<KeyedDataPoint<Integer>> generateSensorData(StreamExecutionEnvironment env) {

        // Some boiler plate settings
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
        env.setParallelism(1);

        // This must be disabled!!! Otherwise faulty timestamps.
        //env.disableOperatorChaining();

        // Initial data - just timestamped messages
        DataStreamSource<DataPoint<Long>> timestampSource =
                env.addSource(new TimestampSource(), "Morse Timestamps");

        // Add resistance data
        SingleOutputStreamOperator<DataPoint<Integer>> morseDataStream = timestampSource
                .map(new ResistanceFunction("resources/sepiapro-morsedata-all.csv"))
                .name("Morse Data");

        // Add keys to DataPoints
        SingleOutputStreamOperator<KeyedDataPoint<Integer>> resistanceStream = morseDataStream
                .map(new AssignKeyFunction("resistance"))
                .name("Resistance Data");

        return resistanceStream;
    }
}
