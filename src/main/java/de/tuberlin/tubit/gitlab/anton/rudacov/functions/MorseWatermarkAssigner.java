package de.tuberlin.tubit.gitlab.anton.rudacov.functions;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class MorseWatermarkAssigner implements AssignerWithPunctuatedWatermarks<KeyedDataPoint<Integer>> {
    @Override
    public Watermark checkAndGetNextWatermark(KeyedDataPoint<Integer> dataPoint, long l) {

        return new Watermark(dataPoint.getTimeStampMs() - 2000);
    }

    @Override
    public long extractTimestamp(KeyedDataPoint<Integer> dataPoint, long l) {

        return dataPoint.getTimeStampMs();
    }
}
