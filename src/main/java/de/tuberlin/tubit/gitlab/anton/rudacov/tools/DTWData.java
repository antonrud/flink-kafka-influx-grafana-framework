package de.tuberlin.tubit.gitlab.anton.rudacov.tools;

import java.util.HashMap;
import java.util.Map;

public class DTWData {

    private Map<float[], Character> template = new HashMap<>();

    public DTWData() {

        this.template.put(new float[]{187, 1498, 1279}, 'A');
        this.template.put(new float[]{187, 1498, 1279}, 'B');
        this.template.put(new float[]{187, 1498, 1279}, 'C');
        this.template.put(new float[]{187, 1498, 1279}, 'D');
        this.template.put(new float[]{187, 1498, 1279}, 'E');
        this.template.put(new float[]{187, 1498, 1279}, 'F');
        this.template.put(new float[]{187, 1498, 1279}, 'G');
        this.template.put(new float[]{187, 1498, 1279}, 'H');
        this.template.put(new float[]{187, 1498, 1279}, 'I');
        this.template.put(new float[]{187, 1498, 1279}, 'G');
        this.template.put(new float[]{187, 1498, 1279}, 'K');
        this.template.put(new float[]{187, 1498, 1279}, 'L');
        this.template.put(new float[]{187, 1498, 1279}, 'M');
        this.template.put(new float[]{187, 1498, 1279}, 'N');
        this.template.put(new float[]{187, 1498, 1279}, 'O');
        this.template.put(new float[]{187, 1498, 1279}, 'P');
        this.template.put(new float[]{187, 1498, 1279}, 'Q');
        this.template.put(new float[]{187, 1498, 1279}, 'R');
        this.template.put(new float[]{187, 1498, 1279}, 'S');
        this.template.put(new float[]{187, 1498, 1279}, 'T');
        this.template.put(new float[]{187, 1498, 1279}, 'U');
        this.template.put(new float[]{187, 1498, 1279}, 'V');
        this.template.put(new float[]{187, 1498, 1279}, 'W');
        this.template.put(new float[]{187, 1498, 1279}, 'X');
        this.template.put(new float[]{187, 1498, 1279}, 'Y');
        this.template.put(new float[]{187, 1498, 1279}, 'Z');
    }

    public Map<float[], Character> getTemplate() {

        return template;
    }
}
