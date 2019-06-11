package k.means;

import data.common.LocalFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple4;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class PredictionsShowRanges {

    public static void main(String[] args) {
        final JavaSparkContext jsc = new JavaSparkContext("local", "Anomaly Detection");
        final JavaRDD<String> predictions = jsc.textFile(LocalFiles.firsColumnPredictions);
        final List<Tuple4<Integer, Integer, Double, Double>> ranges =
                predictions
                        .map(s -> s.split("\\|~\\|"))
                        .map(arr -> new Tuple2<>(Integer.parseInt(arr[0]), arr[1]))
                        .map(t -> new Tuple2<>(t._1(), parseCsvList(t._2())))
                        .map(PredictionsShowRanges::countMinMax)
                        .collect();
        System.out.println(
                ranges
                        .stream()
                        .sorted(Comparator.comparing(Tuple4::_3))
                        .map(t -> t._1() + " [" + t._2() + "] |~|  " + t._3() + " , " + t._4())
                        .collect(Collectors.joining("\n"))
        );

        jsc.close();
    }

    private static List<Double> parseCsvList(String s) {
        return Arrays
                .stream(s.split(","))
                .map(Double::parseDouble)
                .collect(Collectors.toList());
    }

    private static Tuple4<Integer, Integer, Double, Double> countMinMax(Tuple2<Integer, List<Double>> tuple) {
        return new Tuple4<>(
                tuple._1(),
                tuple._2().size(),
                Collections.min(tuple._2()),
                Collections.max(tuple._2())
        );
    }
}
