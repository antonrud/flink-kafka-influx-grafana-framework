package de.tuberlin.tubit.gitlab.anton.rudacov.data;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class DataPointSerializationSchema implements SerializationSchema<KeyedDataPoint<Integer>>, DeserializationSchema<KeyedDataPoint<Integer>> {

    @Override
    public byte[] serialize(KeyedDataPoint<Integer> dataPoint) {
        String s = dataPoint.getTimeStampMs() + "," + dataPoint.getKey() + "," + dataPoint.getValue();
        return s.getBytes();
    }

    @Override
    public KeyedDataPoint<Integer> deserialize(byte[] bytes) throws IOException {
        String s = new String(bytes);
        String[] parts = s.split(",");
        long timestampMs = Long.valueOf(parts[0]);
        String key = parts[1];
        int value = Integer.valueOf(parts[2]);
        return new KeyedDataPoint<>(key, timestampMs, value);
    }

    @Override
    public boolean isEndOfStream(KeyedDataPoint<Integer> doubleKeyedDataPoint) {
        return false;
    }

    @Override
    public TypeInformation<KeyedDataPoint<Integer>> getProducedType() {
        return TypeInformation.of(new TypeHint<KeyedDataPoint<Integer>>() {
        });
    }
}
