package de.tuberlin.tubit.gitlab.anton.rudacov.tools;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class GrafanaAnnotation {

    private final String GRAFANA_URL = "http://217.163.23.24:3000/api/annotations";
    private final String GRAFANA_USER = "admin";
    private final String GRAFANA_PASS = "DBPROgruppe3";
    private final int GRAFANA_DASHBOARDID = 4;
    private final int GRAFANA_PANELID = 2;
    private final String GRAFANA_TAG = "character";

    public GrafanaAnnotation(String text, long startTime, long endTime) {

        JSONObject data = new JSONObject();

        data.put("dashboardId", this.GRAFANA_DASHBOARDID);
        data.put("panelId", this.GRAFANA_PANELID);
        data.put("time", startTime);
        data.put("isRegion", true);
        data.put("timeEnd", endTime + 500);
        data.put("tags", new JSONArray("[" + this.GRAFANA_TAG + "]"));
        data.put("text", text);

        this.sendPost(data, startTime, endTime);
    }

    public void sendPost(JSONObject data, long startTime, long endTime) {

        try {
            URL url = new URL(this.GRAFANA_URL);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
            String encoded = Base64.getEncoder().encodeToString((this.GRAFANA_USER + ":" + this.GRAFANA_PASS).getBytes(StandardCharsets.UTF_8));  //Java 8
            httpConnection.setRequestProperty("Authorization", "Basic " + encoded);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestMethod("POST");
            httpConnection.setRequestProperty("Content-Type", "application/json");
            httpConnection.setRequestProperty("Accept", "application/json");

            // Writes the JSON parsed as string to the connection
            DataOutputStream wr = new DataOutputStream(httpConnection.getOutputStream());
            wr.write(data.toString().getBytes());
            Integer responseCode = httpConnection.getResponseCode();

            BufferedReader bufferedReader;

            // Creates a reader buffer
            if (responseCode > 199 && responseCode < 300) {
                bufferedReader = new BufferedReader(new InputStreamReader(httpConnection.getInputStream()));
            } else {
                bufferedReader = new BufferedReader(new InputStreamReader(httpConnection.getErrorStream()));
            }

            // To receive the response
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                content.append(line).append("\n");
            }
            bufferedReader.close();

            // Prints the response
            System.out.println(content.toString());

        } catch (Exception e) {

            System.out.println("[ERROR] Grafana annotation failed!");
            System.out.println(e.getClass().getSimpleName());
            System.out.println(e.getMessage());
        }
    }
}