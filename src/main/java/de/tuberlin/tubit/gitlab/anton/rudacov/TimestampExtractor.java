package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

public class TimestampExtractor implements AssignerWithPeriodicWatermarks<String> {
    
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {

        //Flink will expect a delay of maximum 2 seconds.
        return new Watermark(System.currentTimeMillis() - 2000);
    }

    @Override
    public long extractTimestamp(String s, long l) {

        LocalDateTime dateTime = LocalDateTime.of(LocalDate.now(), LocalTime.parse(s.split(";")[0]));

        return dateTime.atZone(ZoneId.of("Europe/Berlin")).toInstant().toEpochMilli();
    }
}
