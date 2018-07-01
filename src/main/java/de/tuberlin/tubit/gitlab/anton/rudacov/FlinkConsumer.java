package de.tuberlin.tubit.gitlab.anton.rudacov;

<<<<<<< HEAD
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
=======
import de.tuberlin.tubit.gitlab.anton.rudacov.mappers.InfluxDBMapper;
import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.DataPointSerializationSchema;
import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.KeyedDataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.SensorDataWatermarkAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
>>>>>>> 12-timestamps-correction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class FlinkConsumer implements Runnable {

<<<<<<< HEAD

    public FlinkConsumer() {

    }

    public void consume() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", App.KAFKA_BROKER);

        FlinkKafkaConsumer011<String> dataConsumer = new FlinkKafkaConsumer011<String>(App.KAFKA_TOPIC, new SimpleStringSchema(), properties);

        /* Uncomment this if necessary */
        //dataConsumer.setStartFromEarliest();

        DataStream<String> stream = env.addSource(dataConsumer);

        /* InfluxDB sink */
        InfluxDBConfig influxDBConfig = new InfluxDBConfig(InfluxDBConfig.builder(App.INFLUX_URL, App.INFLUX_USER, App.INFLUX_PASS, App.INFLUX_DATABASE));

        stream.map(new InfluxDBMapper())
                .addSink(new InfluxDBSink(influxDBConfig));

        /* Sink to console */
        stream/*.rebalance() this causes strange reorder of messages*/
                .map(x -> x.split(";")[1])
                .map(x -> Integer.parseInt(x.trim()) > 7500 ? 0 : 1)
                .map(x -> "Received state: " + x)
=======
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

        InfluxDBConfig influxDBConfig = new InfluxDBConfig(InfluxDBConfig.builder(App.INFLUX_URL, App.INFLUX_USER, App.INFLUX_PASS, App.INFLUX_DATABASE));

        sensorStream
                .assignTimestampsAndWatermarks(new SensorDataWatermarkAssigner<>())
              .map(new InfluxDBMapper()).addSink(new InfluxDBSink(influxDBConfig));

        sensorStream
                .map(x -> x.getTimeStampMs() + ": " + x.getValue())
                .returns(String.class)
>>>>>>> 12-timestamps-correction
                .print();

        env.execute();
    }

    @Override
    public void run() {
        App.log('i', "Consumer starting...");

        try {
            consume();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
