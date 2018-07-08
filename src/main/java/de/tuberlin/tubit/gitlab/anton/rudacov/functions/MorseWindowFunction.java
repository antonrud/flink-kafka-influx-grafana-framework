package de.tuberlin.tubit.gitlab.anton.rudacov.functions;

import de.tuberlin.tubit.gitlab.anton.rudacov.data.KeyedDataPoint;
import de.tuberlin.tubit.gitlab.anton.rudacov.jobs.App;
import de.tuberlin.tubit.gitlab.anton.rudacov.tools.DTW;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

        //Convert absolute timestamps to relative time intervals
        ArrayList<Float> intervals = new ArrayList<>();
        for (int index = 1; index < sequence.size(); index++) {
            intervals.add((float) (sequence.get(index) - sequence.get(index - 1)));
        }

        //Prepare data for DTW evaluation
        float[] dtwSample = ArrayUtils.toPrimitive(intervals.toArray(new Float[0]));


/* Solution with threshold value

        //Find similar pattern and return respective character
        Optional<Map.Entry<float[], Character>> character = App.DTW
                .entrySet()
                .stream()
                .filter(x -> new DTW(dtwSample, x.getKey()).getDistance() < App.DTW_SENSITIVITY)
                .findAny();

        //Output the result
        if (character.isPresent()) {
            System.out.println("Detected: " + character.get().getValue() + " at " + sequence.get(sequence.size() - 1));
        } else {
            System.out.println("Unrecognized character at " + sequence.get(sequence.size() - 1));
        }
*/

        // Calculate DTW distances
        Map<Double, Character> distances = new HashMap<>();
        App.DTW
                .entrySet()
                .stream()
                .forEach(x -> distances.put(new DTW(dtwSample, x.getKey()).getDistance(), x.getValue()));

        // Get minimal distance
        double minimalDistance = distances
                .entrySet()
                .stream()
                .mapToDouble(x -> x.getKey())
                .min()
                .getAsDouble();

        //TODO Convert to human readable time
        System.out.println("Detected: " + distances.get(minimalDistance) + " at " + sequence.get(sequence.size() - 1));
    }
}