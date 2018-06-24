package de.tuberlin.tubit.gitlab.anton.rudacov.oscon;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class DataPointSerializationSchema<T> implements SerializationSchema<KeyedDataPoint<T>>, DeserializationSchema<KeyedDataPoint<T>>, Serializer<KeyedDataPoint<T>> {
    @Override
    public byte[] serialize(KeyedDataPoint<T> dataPoint) {
        String s =  dataPoint.getTimeStampMs() + "," + dataPoint.getKey() + "," + dataPoint.getValue();
        return s.getBytes();
    }

    @Override
    public KeyedDataPoint<T> deserialize(byte[] bytes) throws IOException {
        String s = new String(bytes);
        String[] parts = s.split(",");
        long timestampMs = Long.valueOf(parts[0].trim());
        String key = parts[1].trim();
        T value = (T) parts[2].trim();
        return new KeyedDataPoint<T>(key, timestampMs, value);
    }

    @Override
    public boolean isEndOfStream(KeyedDataPoint<T> doubleKeyedDataPoint) {
        return false;
    }

    @Override
    public TypeInformation<KeyedDataPoint<T>> getProducedType() {
        return TypeInformation.of(new TypeHint<KeyedDataPoint<T>>(){});
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to do
    }

    @Override
    public byte[] serialize(String topic, KeyedDataPoint<T> data) {
        return this.serialize(data);
    }

    @Override
    public void close() {
        // nothing to do
    }
}