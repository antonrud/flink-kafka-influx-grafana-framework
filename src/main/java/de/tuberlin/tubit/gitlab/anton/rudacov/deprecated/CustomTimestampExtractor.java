package de.tuberlin.tubit.gitlab.anton.rudacov.deprecated;

import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CustomTimestampExtractor implements TimestampExtractor {
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        final long timestamp = record.timestamp();

        if ( timestamp < 0 ) {
            return System.currentTimeMillis();
        }

        return timestamp;
    }

    @Override
    public long extractTimestamp(Object element, long currentTimestamp) {
        return extract((ConsumerRecord<Object, Object>) element, currentTimestamp);
    }

    @Override
    public long extractWatermark(Object element, long currentTimestamp) {
        return getCurrentWatermark();
    }

    @Override
    public long getCurrentWatermark() {
        return System.currentTimeMillis();
    }
}