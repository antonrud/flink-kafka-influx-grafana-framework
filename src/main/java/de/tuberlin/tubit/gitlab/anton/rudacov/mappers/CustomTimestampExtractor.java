package de.tuberlin.tubit.gitlab.anton.rudacov.mappers;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class CustomTimestampExtractor implements AssignerWithPunctuatedWatermarks<String>  {
    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        //System.out.println(extractedTimestamp + " / " + lastElement);
        return new Watermark(extractedTimestamp);
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        //System.out.println(previousElementTimestamp);
        return previousElementTimestamp;
    }
}