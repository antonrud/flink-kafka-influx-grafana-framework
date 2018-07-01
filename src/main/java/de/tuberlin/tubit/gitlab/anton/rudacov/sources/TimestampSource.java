package de.tuberlin.tubit.gitlab.anton.rudacov.sources;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.DataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Collections;
import java.util.List;

public class TimestampSource extends RichSourceFunction<DataPoint<Long>> implements ListCheckpointed<Long> {


    private volatile boolean running = true;

    // Checkpointed State
    private volatile long currentTimeMs = 0;
    private volatile int currentRun = 1;

    public TimestampSource() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        long now = System.currentTimeMillis();
        if (currentTimeMs == 0) {
            currentTimeMs = now - (now % 1000); // floor to second boundary
        }
    }

    @Override
    public void run(SourceContext<DataPoint<Long>> ctx) throws Exception {
        while (running) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collectWithTimestamp(new DataPoint<>(currentTimeMs, 0L), currentTimeMs);
                ctx.emitWatermark(new Watermark(currentTimeMs));

                currentTimeMs += 31;

                //This is necessary due to timestamps variations in the source file
                if (currentRun == 5) {
                    currentTimeMs += 1;
                    currentRun = 0;
                }
                currentRun++;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        return Collections.singletonList(this.currentTimeMs);
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
        this.currentTimeMs = state.isEmpty() ? 0 : state.get(0);
    }
}
