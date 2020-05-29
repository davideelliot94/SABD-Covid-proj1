package utils;

import java.util.List;

public class StatisticsComputer {

    public static double variance(List<Double> a,
                                  int n, double mean)
    {

        double sqDiff = 0;
        for (int i = 0; i < n; i++) {
            sqDiff += (a.get(i) - mean) *
                    (a.get(i) - mean);
        }
        double var = sqDiff / (double)n;
        return var;
    }

    public static double mean(List<Double> a,
                              int n)
    {

        double sum = 0;

        for (int i = 0; i < n; i++) {
            sum += a.get(i);
        }
        double mean = sum /
                (double)n;

        return mean;
    }

    public static double getMax(List<Double> a,
                                int n)
    {

        double max = 0;

        for (int i = 0; i < n; i++){
            if(max < a.get(i) ){
                max = a.get(i);
            }
        }
        return max;
    }

    public static double getMin(List<Double> a,
                                int n)
    {

        double min = Double.POSITIVE_INFINITY;

        for (int i = 0; i < n; i++){
            if(min > a.get(i) ){
                min = a.get(i);
            }
        }
        return min;
    }

}
