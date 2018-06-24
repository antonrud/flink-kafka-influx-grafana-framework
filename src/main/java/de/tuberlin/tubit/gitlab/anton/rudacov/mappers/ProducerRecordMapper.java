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

        String key = line.split(";")[0];
        String value = line.split(";")[1];
        LocalDateTime dateTime = LocalDateTime.of(LocalDate.now(), LocalTime.parse(key));
        long timestamp = dateTime.atZone(ZoneId.of("Europe/Berlin")).toInstant().toEpochMilli();
        KeyedDataPoint<String> keyedDataPoint = new KeyedDataPoint<>(key, timestamp, value);

        return new ProducerRecord(App.KAFKA_TOPIC, 0 /*Migth be wrong*/, timestamp, key, keyedDataPoint);
    }
}
