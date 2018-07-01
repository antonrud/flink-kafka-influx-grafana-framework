package de.tuberlin.tubit.gitlab.anton.rudacov.jobs;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.DataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.functions.AssignKeyFunction;
import de.tuberlin.tubit.gitlab.anton.rudacov.functions.ResistanceFunction;
import de.tuberlin.tubit.gitlab.anton.rudacov.sinks.InfluxDBSink;
import de.tuberlin.tubit.gitlab.anton.rudacov.sources.TimestampSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class App {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable fault-tolerance for state
        env.enableCheckpointing(1000);

        // Enable Event Time processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Generate stream with sensor data
        DataStream<KeyedDataPoint<Integer>> sensorStream = generateSensorData(env);

        // Uncomment this to check the stream in the console
        //sensorStream.print();

        // Writes sensor stream out to InfluxDB
        //TODO Rewrite Influx Sink
        sensorStream
                .addSink(new InfluxDBSink<>("morse"));


        //TODO Replace this by Morse interpretation logic and sink to Influx
        // Compute a windowed sum over this data and write that to InfluxDB as well.
/*        sensorStream
                .keyBy("key")
                .timeWindow(Time.seconds(1))
                .sum("value")
                .addSink(new InfluxDBSink<>("summedSensors"));  */


        // Execute Flink
        env.execute("Morse code");
    }

    private static DataStream<KeyedDataPoint<Integer>> generateSensorData(StreamExecutionEnvironment env) {

        // Some boiler plate settings
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
        env.setParallelism(1);
        env.disableOperatorChaining();

        // Initial data - just timestamped messages
        DataStreamSource<DataPoint<Long>> timestampSource =
                env.addSource(new TimestampSource(), "Morse Timestamps");

        // Transform into sawtooth pattern
        SingleOutputStreamOperator<DataPoint<Integer>> morseDataStream = timestampSource
                .map(new ResistanceFunction("resources/sepiapro-morsedata-all.csv"))
                .name("Morse Data");

        // Simulate temp sensor
        SingleOutputStreamOperator<KeyedDataPoint<Integer>> resistanceStream = morseDataStream
                .map(new AssignKeyFunction("resistance"))
                .name("Resistance Data");

        return resistanceStream;
    }
}
