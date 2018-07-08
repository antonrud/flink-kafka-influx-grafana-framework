package de.tuberlin.tubit.gitlab.anton.rudacov.tools;

import java.util.HashMap;
import java.util.Map;

public class DTWData {

    private Map<float[], Character> template = new HashMap<>();

    public DTWData() {

        this.template.put(new float[]{187, 1435, 1435}, 'A');
        this.template.put(new float[]{1154, 1623, 281, 530, 343, 499, 406}, 'B');
        this.template.put(new float[]{936, 1092, 156, 1061, 905, 686, 219}, 'C');
        this.template.put(new float[]{1092, 1248, 343, 468, 343}, 'D');
        this.template.put(new float[]{156}, 'E');
        this.template.put(new float[]{156, 561, 156, 905, 1310, 1124, 156}, 'F');
        this.template.put(new float[]{1092, 718, 1248, 780, 187}, 'G');
        this.template.put(new float[]{250, 405, 281, 437, 312, 437, 281}, 'H');
        this.template.put(new float[]{187, 686, 219}, 'I');
        this.template.put(new float[]{219, 1061, 873, 999, 1248, 1092, 1248}, 'G');
        this.template.put(new float[]{1248, 593, 343, 718, 1529}, 'K');
        this.template.put(new float[]{219, 530, 1186, 624, 218, 718, 249}, 'L');
        this.template.put(new float[]{1623, 1061, 1528}, 'M');
        this.template.put(new float[]{1373, 749, 312}, 'N');
        this.template.put(new float[]{811, 936, 936, 1030, 904}, 'O');
        this.template.put(new float[]{312, 749, 1030, 936, 967, 905, 187}, 'P');
        this.template.put(new float[]{1092, 718, 1248, 780, 187, 676, 1230}, 'Q');
        this.template.put(new float[]{137, 1135, 1435, 1344, 150}, 'R');
        this.template.put(new float[]{281, 526, 353, 502, 376}, 'S');
        this.template.put(new float[]{1430}, 'T');
        this.template.put(new float[]{153, 551, 157, 887, 1311}, 'U');
        this.template.put(new float[]{281, 526, 353, 502, 376, 1092, 1248}, 'V');
        this.template.put(new float[]{156, 861, 1623, 1061, 1528}, 'W');
        this.template.put(new float[]{1092, 1248, 343, 468, 343, 1148, 1201}, 'X');
        this.template.put(new float[]{1132, 1148, 219, 1061, 1073, 999, 1248}, 'Y');
        this.template.put(new float[]{811, 936, 936, 837, 212, 637, 181}, 'Z');
    }

    public Map<float[], Character> getTemplate() {

        return template;
    }
}
