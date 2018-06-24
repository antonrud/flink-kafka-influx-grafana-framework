package de.tuberlin.tubit.gitlab.anton.rudacov.oscon;

import de.tuberlin.tubit.gitlab.anton.rudacov.App;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class InfluxDBSink<T extends DataPoint<? extends Number>> extends RichSinkFunction<T> {

    private transient InfluxDB influxDB = null;
    private static String dataBaseName = App.INFLUX_DATABASE;
    private static String fieldName = "resistance";     /** anpassen */
    private String measurement;
    private Integer threshold;

    public InfluxDBSink(String measurement){
        this.measurement = measurement;
    }
    public InfluxDBSink(String measurement, Integer threshold){
        this.measurement = measurement;
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        influxDB = InfluxDBFactory.connect(App.INFLUX_URL, App.INFLUX_USER, App.INFLUX_PASS);
        influxDB.createDatabase(dataBaseName);
        influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(T dataPoint) throws Exception {
        Integer val = Integer.parseInt(String.valueOf(dataPoint.getValue()).trim()) > this.threshold ? 0 : 1;
        Point.Builder builder = Point.measurement(measurement)
                .time(dataPoint.getTimeStampMs(), TimeUnit.MILLISECONDS)
                .addField(fieldName, val);
                //.addField(fieldName, dataPoint.getValue());

        if(dataPoint instanceof KeyedDataPoint){
            builder.tag("key", ((KeyedDataPoint) dataPoint).getKey());
        }

        Point p = builder.build();

        influxDB.write(dataBaseName, "autogen", p);
    }
}