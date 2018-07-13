package de.tuberlin.tubit.gitlab.anton.rudacov.tools;

/**
 * This class implements the Dynamic Time Warping algorithm
 * given two sequences
 * <pre>
 *   X = x1, x2,..., xi,..., xn
 *   Y = y1, y2,..., yj,..., ym
 *  </pre>
 *
 * @author Cheol-Woo Jung (cjung@gatech.edu)
 * @version 1.0
 */
public class DTW {

    protected int m;
    private float[] seq1;
    private float[] seq2;
    private int[][] warpingPath;
    private int n;
    private int K;

    private double warpingDistance;

    public DTW(float[] sample, float[] templete) {
        if (sample.length == templete.length) {

            seq1 = sample;
            seq2 = templete;

            n = seq1.length;
            m = seq2.length;
            K = 1;

            warpingPath = new int[n + m][2];    // max(n, m) <= K < n + m
            warpingDistance = 0.0;

            this.compute();
        } else {

            this.warpingDistance = Double.MAX_VALUE;
        }
    }

    public static void main(String[] args) {

        // Just an example
        //float[] n2 = {1.5f, 3.9f, 4.1f, 3.3f};
        //float[] n1 = {2.1f, 2.45f, 3.673f, 4.32f, 2.05f, 1.93f, 5.67f, 6.01f};
        //DTW dtw = new DTW(n1, n2);
        //System.out.println(dtw);

//Uncomment this for debugging
/**
 ArrayList<Double> list = new ArrayList<>();

 for (Map.Entry<float[], Character> entry : App.DTW.entrySet()) {

 for (Map.Entry<float[], Character> entry2 : App.DTW.entrySet()) {

 double distance = new DTW(entry.getKey(), entry2.getKey()).getDistance();

 System.out.println(distance);
 list.add(distance);

 }

 System.out.println();
 System.out.println();
 }

 System.out.println();
 System.out.println();
 System.out.println();
 System.out.println();

 list.stream().sorted().forEach(System.out::println);
 */

    }

    private void compute() {
        double accumulatedDistance = 0.0;

        double[][] d = new double[n][m];    // local distances
        double[][] D = new double[n][m];    // global distances

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                d[i][j] = distanceBetween(seq1[i], seq2[j]);
            }
        }

        D[0][0] = d[0][0];

        for (int i = 1; i < n; i++) {
            D[i][0] = d[i][0] + D[i - 1][0];
        }

        for (int j = 1; j < m; j++) {
            D[0][j] = d[0][j] + D[0][j - 1];
        }

        for (int i = 1; i < n; i++) {
            for (int j = 1; j < m; j++) {
                accumulatedDistance = Math.min(Math.min(D[i - 1][j], D[i - 1][j - 1]), D[i][j - 1]);
                accumulatedDistance += d[i][j];
                D[i][j] = accumulatedDistance;
            }
        }
        accumulatedDistance = D[n - 1][m - 1];

        int i = n - 1;
        int j = m - 1;
        int minIndex = 1;

        warpingPath[K - 1][0] = i;
        warpingPath[K - 1][1] = j;

        while ((i + j) != 0) {
            if (i == 0) {
                j -= 1;
            } else if (j == 0) {
                i -= 1;
            } else {    // i != 0 && j != 0
                double[] array = {D[i - 1][j], D[i][j - 1], D[i - 1][j - 1]};
                minIndex = this.getIndexOfMinimum(array);

                if (minIndex == 0) {
                    i -= 1;
                } else if (minIndex == 1) {
                    j -= 1;
                } else if (minIndex == 2) {
                    i -= 1;
                    j -= 1;
                }
            } // end else
            K++;
            warpingPath[K - 1][0] = i;
            warpingPath[K - 1][1] = j;
        } // end while
        warpingDistance = accumulatedDistance / K;

        this.reversePath(warpingPath);
    }

    /**
     * Changes the order of the warping path (increasing order)
     *
     * @param path the warping path in reverse order
     */
    private void reversePath(int[][] path) {
        int[][] newPath = new int[K][2];
        for (int i = 0; i < K; i++) {
            for (int j = 0; j < 2; j++) {
                newPath[i][j] = path[K - i - 1][j];
            }
        }
        warpingPath = newPath;
    }

    /**
     * Returns the warping distance
     *
     * @return
     */
    public double getDistance() {
        return warpingDistance;
    }

    /**
     * Computes a distance between two points
     *
     * @param p1 the point 1
     * @param p2 the point 2
     * @return the distance between two points
     */
    private double distanceBetween(double p1, double p2) {
        return (p1 - p2) * (p1 - p2);
    }

    /**
     * Finds the index of the minimum element from the given array
     *
     * @param array the array containing numeric values
     * @return the min value among elements
     */
    private int getIndexOfMinimum(double[] array) {
        int index = 0;
        double val = array[0];

        for (int i = 1; i < array.length; i++) {
            if (array[i] < val) {
                val = array[i];
                index = i;
            }
        }
        return index;
    }

    /**
     * Returns a string that displays the warping distance and path
     */
    public String toString() {
        String retVal = "Warping Distance: " + warpingDistance + "\n";
        retVal += "Warping Path: {";
        for (int i = 0; i < K; i++) {
            retVal += "(" + warpingPath[i][0] + ", " + warpingPath[i][1] + ")";
            retVal += (i == K - 1) ? "}" : ", ";

        }

        return retVal;
    }
}