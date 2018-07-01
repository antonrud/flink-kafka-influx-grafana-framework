package de.tuberlin.tubit.gitlab.anton.rudacov.jobs;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.DataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.functions.AssignKeyFunction;
import de.tuberlin.tubit.gitlab.anton.rudacov.functions.SawtoothFunction;
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

        // Uncomment this line to enable fault-tolerance for state
        env.enableCheckpointing(1000);

        // Uncomment this line to enable Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Simulate some sensor data
        DataStream<KeyedDataPoint<Double>> sensorStream = generateSensorData(env);

        // Write this sensor stream out to InfluxDB
        sensorStream
                .addSink(new InfluxDBSink<>("morse"));


        //TODO Replace this by Morse interpretation logic
        // Compute a windowed sum over this data and write that to InfluxDB as well.
/*        sensorStream
                .keyBy("key")
                .timeWindow(Time.seconds(1))
                .sum("value")
                .addSink(new InfluxDBSink<>("summedSensors"));  */


        // Execute Flink
        env.execute("Morse code");
    }

    private static DataStream<KeyedDataPoint<Double>> generateSensorData(StreamExecutionEnvironment env) {

        // Some boiler plate settings
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
        env.setParallelism(1);
        env.disableOperatorChaining();

        // Initial data - just timestamped messages
        DataStreamSource<DataPoint<Long>> timestampSource =
                env.addSource(new TimestampSource(), "Morse timestamps");

        // Transform into sawtooth pattern
        SingleOutputStreamOperator<DataPoint<Double>> sawtoothStream = timestampSource
                .map(new SawtoothFunction(10))
                .name("sawTooth");

        // Simulate temp sensor
        SingleOutputStreamOperator<KeyedDataPoint<Double>> tempStream = sawtoothStream
                .map(new AssignKeyFunction("temp"))
                .name("assignKey(temp)");

        return tempStream;
    }
}
