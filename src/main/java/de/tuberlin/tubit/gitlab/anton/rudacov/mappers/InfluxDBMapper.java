package de.tuberlin.tubit.gitlab.anton.rudacov.mappers;

import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.KeyedDataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.HashMap;

public class InfluxDBMapper extends RichMapFunction<KeyedDataPoint<String>, InfluxDBPoint> implements Serializable {

    @Override
    public InfluxDBPoint map(KeyedDataPoint<String> dataPoint) {

        String measurement = "morseMeasurement";

        long timestamp = dataPoint.getTimeStampMs();

        HashMap<String, String> tags = new HashMap<>();
        tags.put("id", "generated");

        HashMap<String, Object> fields = new HashMap<>();
        fields.put("resistance", dataPoint.getValue());
        fields.put("code", Integer.parseInt(dataPoint.getValue().trim()) > 7500 ? 0 : 1);

        return new InfluxDBPoint(measurement, timestamp, tags, fields);
    }
}