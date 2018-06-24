package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class App {
    
    public static final String KAFKA_BROKER = "217.163.23.24:9092";
    public static final String KAFKA_TOPIC = "morse";

    public static final String INFLUX_URL = "http://217.163.23.24:8086";
    public static final String INFLUX_USER = "admin";
    public static final String INFLUX_PASS = "DBPROgruppe3";
    public static final String INFLUX_DATABASE = "morse";

    private static final String DATA_PATH = "resources/sepiapro-morsedata-all.csv";

    public static void main(String[] args) throws IOException {
        App.log('i', "Yay! App started!");

        /* Drop previous measurements in InfluxDB */
        InfluxDB influxDB = InfluxDBFactory.connect(App.INFLUX_URL, App.INFLUX_USER, App.INFLUX_PASS);
        influxDB.setDatabase(App.INFLUX_DATABASE);
        Query query = new Query("DROP MEASUREMENT morseMeasurement", App.INFLUX_DATABASE);
        influxDB.query(query);
        influxDB.close();
        App.log('i', "Database droped!");

        /* Starting Flink consumer */
        (new Thread(new FlinkConsumer())).start();

        /* Starting data generator */
        //(new Thread(new DataGenerator(DATA_PATH))).start();
        (new Thread(new DataGeneratorTime(DATA_PATH))).start();
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