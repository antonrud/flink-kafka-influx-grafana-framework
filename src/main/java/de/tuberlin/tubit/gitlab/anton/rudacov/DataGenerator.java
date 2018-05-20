package de.tuberlin.tubit.gitlab.anton.rudacov;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class DataGenerator implements Runnable {

    private String dataPath;

    public DataGenerator(String dataPath) {

        this.dataPath = dataPath;
    }

    @Override
    public void run() {
        App.log('i', "Generator starting...");

        Stream<String> dataStream = getDataStream(dataPath);


        while (!Thread.currentThread().isInterrupted()) {


            Thread.currentThread().interrupt();
        }
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
}