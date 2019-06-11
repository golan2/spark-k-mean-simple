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
        readWrite(jsc);
        jsc.close();
    }

    static void readWrite(JavaSparkContext jsc) {
        final JavaRDD<String> rows = jsc.textFile(LocalFiles.firstColumnData);
        final JavaRDD<Long> values = filter(rows);
        values.repartition(1).saveAsTextFile(LocalFiles.firstColumnNormalized);
    }

    public static JavaRDD<Long> filter(JavaRDD<String> rows) {
        final JavaDoubleRDD firstColumn = rows.mapToDouble(Double::parseDouble);
        final double mean = firstColumn.mean();
        final double stdev = firstColumn.stdev();
        final JavaDoubleRDD filtered = firstColumn.filter(new Normalizer(mean, stdev));
        return filtered.map(Math::round);
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
