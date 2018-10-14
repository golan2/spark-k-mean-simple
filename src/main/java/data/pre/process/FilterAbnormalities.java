package data.pre.process;

import data.common.LocalFiles;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class FilterAbnormalities {

    public static void main(String[] args) {
        final JavaSparkContext jsc = new JavaSparkContext("local", "Anomaly Detection");
        final JavaDoubleRDD firstColumn = jsc.textFile(LocalFiles.firstColumnDataPath).mapToDouble(Double::parseDouble);
        final double mean = firstColumn.mean();
        final double stdev = firstColumn.stdev();
        final JavaDoubleRDD filtered = firstColumn.filter(new FilterAbnormalities.Normalizer(mean, stdev));
        final JavaRDD<Long>  values = filtered.map(Math::round);
        values.saveAsTextFile(LocalFiles.firstColumnNormalized);
        jsc.close();
    }


    public static class Normalizer implements Serializable, Function<Double, Boolean> {
        final double mean;
        final double stdev;

        Normalizer(double mean, double stdev) {
            this.mean = mean;
            this.stdev = stdev;
        }

        @Override
        public Boolean call(Double d) {
            return d > (mean - 2 * stdev) && d < (mean + 2 * stdev);
        }
    }
}
