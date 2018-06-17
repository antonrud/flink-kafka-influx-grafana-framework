package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.HashMap;

public class InfluxDBMapper extends RichMapFunction<String, InfluxDBPoint> implements Serializable {

    /*TODO Remove this after timestamp extraction is working*/
    private SimpleDateFormat format = new SimpleDateFormat("hh:mm:ss.SSS");

    @Override
    public InfluxDBPoint map(String s) throws Exception {

        /* TODO extract timestamps from stream itself */
        long timestamp = format.parse(s.split(";")[0]).getTime();
        String measurement = "morseMeasurement";

        HashMap<String, String> tags = new HashMap<>();
        tags.put("id", "generated");

        HashMap<String, Object> fields = new HashMap<>();
        fields.put("resistance", s.split(";")[1]);

        return new InfluxDBPoint(measurement, timestamp, tags, fields);
    }
}
