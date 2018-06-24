package de.tuberlin.tubit.gitlab.anton.rudacov.mappers;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.HashMap;

public class InfluxDBMapper extends RichMapFunction<String, InfluxDBPoint> implements Serializable {

    @Override
    public InfluxDBPoint map(String s) throws Exception {

        /* TODO extract timestamps from stream itself */
        LocalDateTime dateTime = LocalDateTime.of(LocalDate.now(), LocalTime.parse(s.split(";")[0]));
        long timestamp = dateTime.atZone(ZoneId.of("Europe/Berlin")).toInstant().toEpochMilli();

        String measurement = "morseMeasurement";

        HashMap<String, String> tags = new HashMap<>();
        tags.put("id", "generated");

        HashMap<String, Object> fields = new HashMap<>();
        fields.put("resistance", s.split(";")[1]);
        fields.put("code", Integer.parseInt(s.split(";")[1].trim()) > 7500 ? 0 : 1);

        return new InfluxDBPoint(measurement, timestamp, tags, fields);
    }
}