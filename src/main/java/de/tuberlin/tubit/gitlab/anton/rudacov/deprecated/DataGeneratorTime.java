package de.tuberlin.tubit.gitlab.anton.rudacov.deprecated;

<<<<<<< HEAD:src/main/java/de/tuberlin/tubit/gitlab/anton/rudacov/deprecated/DataGeneratorTime.java
import de.tuberlin.tubit.gitlab.anton.rudacov.App;
=======
import de.tuberlin.tubit.gitlab.anton.rudacov.mappers.ProducerRecordMapper;
import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.DataPointSerializationSchema;
import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.KeyedDataPoint;
>>>>>>> 12-timestamps-correction:src/main/java/de/tuberlin/tubit/gitlab/anton/rudacov/DataGeneratorTime.java
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

        this.producer = new KafkaProducer<>(properties);
    }

    private void produce() throws IOException {

        Function<String, ProducerRecord> producerRecordMapper = ProducerRecordMapper::apply;

        Stream<String> dataStream = Files.lines(Paths.get(dataPath));
        dataStream
                .map(producerRecordMapper)
                .forEach(record -> producer.send(record));

        producer.close();
        dataStream.close();
    }

    @Override
    public void run() {
        try {
            App.log('i', "Generator starting...");
            produce();
            App.log('i', "Generator ending...");
        } catch (IOException e) {
            App.log('f', "Not able to access data file");
            e.printStackTrace();
        }
    }
}