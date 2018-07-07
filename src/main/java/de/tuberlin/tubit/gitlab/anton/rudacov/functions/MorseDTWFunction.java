package de.tuberlin.tubit.gitlab.anton.rudacov.functions;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MorseDTWFunction implements AllWindowFunction<KeyedDataPoint<Integer>, String, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<KeyedDataPoint<Integer>> values, Collector<String> out) throws Exception {

        //TODO Perform DTW matching
        values.forEach(System.out::println);
        System.out.println("---=== END OF WINDOW ===----");
        System.out.println("---=== END OF WINDOW ===----");
        System.out.println("---=== END OF WINDOW ===----");
    }
}
