package de.tuberlin.tubit.gitlab.anton.rudacov.tools;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;


public class MeasurementDrop {

    private static final String INFLUX_URL = "http://217.163.23.24:8086";
    private static final String INFLUX_USER = "admin";
    private static final String INFLUX_PASS = "DBPROgruppe3";
    private static final String INFLUX_DATABASE = "morse";
    private static final String INFLUX_MEASUREMENT = "morse";

    private MeasurementDrop() {
        // This class contains only static methods
    }

    public static void drop() {

        /* Drop previous measurements in InfluxDB */
        InfluxDB influxDB = InfluxDBFactory.connect(INFLUX_URL, INFLUX_USER, INFLUX_PASS);
        influxDB.setDatabase(INFLUX_DATABASE);
        Query query = new Query("DROP MEASUREMENT " + INFLUX_MEASUREMENT, INFLUX_DATABASE);
        influxDB.query(query);
        influxDB.close();
        System.out.println("[INFO] Measurement droped!");
    }
}
