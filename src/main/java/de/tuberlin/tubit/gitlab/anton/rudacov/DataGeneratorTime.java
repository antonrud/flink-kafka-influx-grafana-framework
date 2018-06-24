package de.tuberlin.tubit.gitlab.anton.rudacov;

import de.tuberlin.tubit.gitlab.anton.rudacov.mappers.ProducerRecordMapper;
import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.DataPointSerializationSchema;
import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.KeyedDataPoint;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Stream;

public class DataGeneratorTime implements Runnable {

    private Properties properties;
    private String dataPath;
    private Producer<String, KeyedDataPoint<String>> producer;

    public DataGeneratorTime(String dataPath) {

        this.dataPath = dataPath;
        this.properties = new Properties();
        this.properties.setProperty("bootstrap.servers", App.KAFKA_BROKER);
        this.properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.properties.setProperty("value.serializer", DataPointSerializationSchema.class.getCanonicalName());
    }

    private void produce() throws Exception {

        Function<String, ProducerRecord> producerRecordMapper = ProducerRecordMapper::apply;
        Stream<String> dataStream = null;

        try {
            producer = new KafkaProducer<>(properties);

            dataStream = Files.lines(Paths.get(dataPath));
            dataStream
                    .map(producerRecordMapper)
                    .forEach(record -> producer.send(record));

            this.producer.close();

        } catch (IOException e) {
            App.log('f', "Not able to access data file");
        }

        dataStream.close();
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