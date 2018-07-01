package de.tuberlin.tubit.gitlab.anton.rudacov.functions;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.DataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import org.apache.flink.api.common.functions.MapFunction;

public class AssignKeyFunction implements MapFunction<DataPoint<Integer>, KeyedDataPoint<Integer>> {

    private String key;

    public AssignKeyFunction(String key) {
        this.key = key;
    }

    @Override
    public KeyedDataPoint<Integer> map(DataPoint<Integer> dataPoint) throws Exception {
        return dataPoint.withKey(key);
    }
}
