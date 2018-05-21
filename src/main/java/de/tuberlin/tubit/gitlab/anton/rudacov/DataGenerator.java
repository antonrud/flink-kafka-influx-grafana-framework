/*package de.tuberlin.tubit.gitlab.anton.rudacov;

import java.util.stream.Stream;

public class DataGenerator implements Runnable {

     Standart args set:
    --topic
    test
    --bootstrap.servers
    localhost:9092


    String[] args;
    private String dataPath;

    public DataGenerator(String dataPath) {

        this.dataPath = dataPath;
    }

    private void produce() throws Exception {
        Stream<String> dataStream = getDataStream(dataPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());

        // write stream to Kafka
        messageStream.addSink(new KafkaSink(parameterTool.getRequired("bootstrap.servers"),
                parameterTool.getRequired("topic"),
                new SimpleStringSchema()));

        env.execute();
    }

    private Stream getDataStream(String file) {
        Stream<String> dataStream = null;

        try {
            dataStream = Files.lines(Paths.get(file));
        } catch (IOException e) {
            App.log('f', "Not able to access data file");
        }

        return dataStream;
    }

    @Override
    public void run() {
        App.log('i', "Generator starting...");

        try {
            produce();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class SimpleStringGenerator implements SourceFunction<String> {
        private static final long serialVersionUID = 2174904787118597072L;
        boolean running = true;
        long i = 0;

        @Override
        public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
            while (running) {
                ctx.collect("element-" + (i++));
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
*/