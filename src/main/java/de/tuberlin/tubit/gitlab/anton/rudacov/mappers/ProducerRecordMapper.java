package de.tuberlin.tubit.gitlab.anton.rudacov.mappers;

import de.tuberlin.tubit.gitlab.anton.rudacov.App;
import de.tuberlin.tubit.gitlab.anton.rudacov.oscon.KeyedDataPoint;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;


public class ProducerRecordMapper {

    public static ProducerRecord apply(String line) {

        String serializedKey = line.split(";")[0];
        String serializedValue = line.split(";")[1];
        LocalDateTime dateTime = LocalDateTime.of(LocalDate.now(), LocalTime.parse(line.split(";")[0]));
        long timestamp = dateTime.atZone(ZoneId.of("Europe/Berlin")).toInstant().toEpochMilli();
        KeyedDataPoint<String> dp = new KeyedDataPoint<>(serializedKey, timestamp, serializedValue);

        return new ProducerRecord(App.KAFKA_TOPIC, 0 /*Migth be wrong*/, timestamp, serializedKey, dp);
    }
}
