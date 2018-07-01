package de.tuberlin.tubit.gitlab.anton.rudacov.functions;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.DataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.Collections;
import java.util.List;

public class SawtoothFunction extends RichMapFunction<DataPoint<Long>, DataPoint<Double>> implements ListCheckpointed<Integer> {

    final private int numSteps;

    // State!
    private int currentStep;

    public SawtoothFunction(int numSteps) {
        this.numSteps = numSteps;
        this.currentStep = 0;
    }

    @Override
    public DataPoint<Double> map(DataPoint<Long> dataPoint) throws Exception {
        double phase = (double) currentStep / numSteps;
        currentStep = ++currentStep % numSteps;
        return dataPoint.withNewValue(phase);
    }

    @Override
    public List<Integer> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        return Collections.singletonList(this.currentStep);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        this.currentStep = state.isEmpty() ? 0 : state.get(0);
    }
}
