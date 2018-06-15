package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TimestampExtractor implements AssignerWithPeriodicWatermarks<String> {

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {

        //Flink will expect a delay of maximum 2 seconds.
        return new Watermark(System.currentTimeMillis() - 2000);
    }

    @Override
    public long extractTimestamp(String s, long l) {

        SimpleDateFormat format = new SimpleDateFormat("hh:mm:ss.SSS");

        String time = s.split(";")[0];

        Long timestamp = null;
        try {
            timestamp = format.parse(time).getTime();
        } catch (ParseException e) {
            App.log('e',"Could not parse event time from data.");
            e.printStackTrace();
        }

        return timestamp;
    }
}
