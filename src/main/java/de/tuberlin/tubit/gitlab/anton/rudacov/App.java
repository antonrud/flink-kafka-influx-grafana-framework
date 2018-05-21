package de.tuberlin.tubit.gitlab.anton.rudacov;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class App {

    private static final String DATA_PATH = "resources/sepiapro-morsedata-all.csv";

    public static void main(String[] args) {
        App.log('i', "Yay! App started!");

        for (String str : args) {
            System.out.println(str);
        }

        /* Starting data generator */
        //(new Thread(new DataGenerator(DATA_PATH))).start();

        /* Starting Flink consumer */
        (new Thread(new FlinkConsumer(args))).start();

    }

    public static void log(char type, String message) {

        String logEvent = "";

        switch (type) {
            case 'i':
                logEvent = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + " [INFO] " + message;
                break;
            case 'w':
                logEvent = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + " [WARNING] " + message;
                break;
            case 's':
                logEvent = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + " [SUCCESS] " + message;
                break;
            case 'f':
                logEvent = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + " [FAIL] " + message;
                break;
            case 'e':
                logEvent = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + " [ERROR] " + message;
                break;
            default:
                logEvent = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + " " + message;
        }

        try {
            File file = new File("log.txt");
            file.createNewFile();

            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            bufferedWriter.write(logEvent);
            bufferedWriter.newLine();

            bufferedWriter.close();
            fileWriter.close();
        } catch (IOException e) {
            App.log('f', "Could not write log event to file");
        }

        System.out.println(logEvent);
    }
}