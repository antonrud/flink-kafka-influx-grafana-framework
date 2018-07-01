package de.tuberlin.tubit.gitlab.anton.rudacov.sinks;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.DataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class InfluxDBSink<T extends DataPoint<? extends Number>> extends RichSinkFunction<T> {

    private static String dataBaseName = "morse";
    private static String fieldName = "value";
    private transient InfluxDB influxDB = null;

    private String measurement;

    public InfluxDBSink(String measurement) {

        this.measurement = measurement;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        influxDB = InfluxDBFactory.connect("http://217.163.23.24:8086", "admin", "DBPROgruppe3");
        influxDB.setDatabase(dataBaseName);
        influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(T dataPoint, SinkFunction.Context context) throws Exception {

        Point.Builder builder = Point.measurement(measurement)
                .time(dataPoint.getTimeStampMs(), TimeUnit.MILLISECONDS)
                .addField(fieldName, dataPoint.getValue());

        if (dataPoint instanceof KeyedDataPoint) {
            builder.tag("key", ((KeyedDataPoint) dataPoint).getKey());
        }

        Point point = builder.build();

        influxDB.write(dataBaseName, "autogen", point);
    }
}
