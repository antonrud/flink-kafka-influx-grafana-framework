package de.tuberlin.tubit.gitlab.anton.rudacov.sinks;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.DataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class InfluxDBSink<T extends DataPoint<? extends Number>> extends RichSinkFunction<T> {

    private static String dataBaseName = "morse";
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
        influxDB.enableBatch(BatchOptions.DEFAULTS);
        influxDB.setLogLevel(InfluxDB.LogLevel.FULL);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(T dataPoint, SinkFunction.Context context) throws Exception {

        Point.Builder builder = Point.measurement(measurement)
                .time(dataPoint.getTimeStampMs(), TimeUnit.MILLISECONDS)
                .addField("value", dataPoint.getValue())
                .addField("code", dataPoint.getValue().intValue() > 7500 ? 0 : 1);

        if (dataPoint instanceof KeyedDataPoint) {
            builder.tag("key", ((KeyedDataPoint) dataPoint).getKey());
        }

        Point point = builder.build();

        influxDB.write(point);
    }
}
