package de.tuberlin.tubit.gitlab.anton.rudacov.oscon;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class SensorDataWatermarkAssigner<T> implements AssignerWithPunctuatedWatermarks<KeyedDataPoint<T>> {
    @Override
    public Watermark checkAndGetNextWatermark(KeyedDataPoint<T> dataPoint, long l) {
        return new Watermark(dataPoint.getTimeStampMs() - 2000);
    }

    @Override
    public long extractTimestamp(KeyedDataPoint<T> doubleKeyedDataPoint, long l) {
        return doubleKeyedDataPoint.getTimeStampMs();
    }
}
