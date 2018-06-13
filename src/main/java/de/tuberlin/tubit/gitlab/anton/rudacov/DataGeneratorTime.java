package de.tuberlin.tubit.gitlab.anton.rudacov;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.stream.Stream;

public class DataGeneratorTime implements Runnable {

    Properties properties;
    private String dataPath;
    private Producer<String, String> producer;

    public DataGeneratorTime(String dataPath) {
        this.dataPath = dataPath;
    }

    private void produce() throws Exception {
        this.properties = new Properties();
        this.properties.setProperty("bootstrap.servers", App.KAFKA_BROKER);
        this.properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        this.properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //
        Stream<String> dataStream = null;
        try{

            dataStream = Files.lines(Paths.get(dataPath));
            this.producer = new KafkaProducer<>(this.properties);

            dataStream.forEach(element -> sendLine(element));
            this.producer.close();
        } catch (IOException e) {
            App.log('f', "Not able to access data file");
        }
        //Stream<String> dataStream = getDataStream(dataPath);
        //dataStream.forEach(element -> sendLine(element));
        dataStream.close();
    }

    private Stream getDataStream(String file) {
        Stream<String> dataStream = null;
        try {
            dataStream = Files.lines(Paths.get(file));
        } catch (IOException e) {
            App.log('f', "Not able to access data file");
            System.out.println("Not able to access data file");
        }
        System.out.println(dataStream.count());
        return dataStream;
    }

    private void sendLine(String line) {
        String[] parts = line.split(";");
        Long timestamp = getTimestamp(parts[0]);
        String serializedKey = App.KAFKA_TOPIC + parts[0];
        String serializedValue = parts[1];
        //
        this.producer.send(new ProducerRecord<String, String>(App.KAFKA_TOPIC, (Integer) null, timestamp, serializedKey, serializedValue));
        //this.producer.send(new ProducerRecord<>(App.KAFKA_TOPIC,serializedValue));
        //App.log('i', "line produced ...");
        //System.out.println("produced");
    }

    private Long getTimestamp(String part){
        try {
            SimpleDateFormat sdf = new java.text.SimpleDateFormat ("HH:mm:ss.SSS");
            Date date = sdf.parse(part);

            return date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void run() {
        try {
            App.log('i', "Generator starting...");
            produce();
            App.log('i', "Generator ending...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/*
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class CustomTimestampExtractor implements TimestampExtractor{
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        final long timestamp = consumerRecord.timestamp();

        if ( timestamp < 0 ) {
            return System.currentTimeMillis();
        }

        return timestamp;
    }
}
 */