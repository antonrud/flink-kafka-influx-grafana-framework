package de.tuberlin.tubit.gitlab.anton.rudacov;

import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.DataPointSerializationSchema;
import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.InfluxDBSink;
import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.KeyedDataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.SensorDataWatermarkAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class FlinkConsumer implements Runnable {

    private Properties properties;

    public FlinkConsumer() {

        this.properties = new Properties();

        this.properties.put("bootstrap.servers", App.KAFKA_BROKER);
        this.properties.put("auto.offset.reset", "earliest");
        this.properties.put("group.id", App.KAFKA_TOPIC);
        this.properties.put("enable_auto_commit", "true");
        this.properties.setProperty("key.serializer", DataPointSerializationSchema.class.getCanonicalName());
        this.properties.setProperty("value.serializer", DataPointSerializationSchema.class.getCanonicalName());
    }

    public void consume() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.enableCheckpointing(1000);
        //env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<KeyedDataPoint<String>> sensorStream =
                env.addSource(new FlinkKafkaConsumer011<>(App.KAFKA_TOPIC, new DataPointSerializationSchema<>(), properties));

        sensorStream
                .assignTimestampsAndWatermarks(new SensorDataWatermarkAssigner<>())
                .addSink(new InfluxDBSink("morseMeasurement", 7500));

        sensorStream
                .map(x -> x.getValue() + " - " + String.valueOf(x.getTimeStampMs()))
                .returns(String.class)
                .print();

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
