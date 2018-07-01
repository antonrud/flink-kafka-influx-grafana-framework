package de.tuberlin.tubit.gitlab.anton.rudacov.oscon;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class KeyedDataPoint<T> extends DataPoint<T> implements ResultTypeQueryable {

    private String key;

    public KeyedDataPoint(){

        super();
        this.key = null;
    }

    public KeyedDataPoint(String key, long timeStampMs, T value) {
        super(timeStampMs, value);
        this.key = key;
    }

    @Override
    public String toString() {
        return getTimeStampMs() + "," + getKey() + "," + getValue();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public <R> KeyedDataPoint<R> withNewValue(R newValue){
        return new KeyedDataPoint<>(this.getKey(), this.getTimeStampMs(), newValue);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeExtractor.getForClass(String.class);
    }
}