package de.tuberlin.tubit.gitlab.anton.rudacov.functions;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class MorseWindowFunction extends ProcessAllWindowFunction<KeyedDataPoint<Integer>, String, TimeWindow> {

    @Override
    public void process(Context context, Iterable<KeyedDataPoint<Integer>> elements, Collector<String> out) throws Exception {

        ArrayList<Long> sequence = new ArrayList<>();

        //Convert to List for easier computation below
        ArrayList<KeyedDataPoint<Integer>> values = Lists.newArrayList(elements);

        //Add first timestamp to sequence
        sequence.add(values.get(0).getTimeStampMs());

        //Find and add input interruption points
        for (int index = 1; index < values.size(); index++) {
            if (values.get(index).getTimeStampMs() - values.get(index - 1).getTimeStampMs() > 40) {
                sequence.add(values.get(index - 1).getTimeStampMs());
                sequence.add(values.get(index).getTimeStampMs());
            }
        }

        //Add last timestamp to sequence
        sequence.add(values.get(values.size() - 1).getTimeStampMs());

        ArrayList<Long> intervals = new ArrayList<>();
        for (int index = 1; index < sequence.size(); index++) {
            intervals.add(sequence.get(index) - sequence.get(index - 1));
        }


        sequence.forEach(System.out::println);
        intervals.forEach(System.out::println);
        System.out.println("---=== NEXT ===---");
        //TODO Apply DTW on this.sequence

    }
}
