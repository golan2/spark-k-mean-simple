package k.means;

import data.common.LocalFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class KmeansPredict {

    public static void main(String[] args) {
        final JavaSparkContext jsc = new JavaSparkContext("local", "Anomaly Detection");
        final KMeansModel model = KMeansModel.load(jsc.sc(), LocalFiles.firstColumnModel);
        final JavaRDD<String> firstColumns = jsc.textFile(LocalFiles.firstColumnDataPath);
        final JavaRDD<Vector> vectors = firstColumns.map(s -> Vectors.dense(Double.parseDouble(s)));
        final JavaPairRDD<Integer, Vector> predictions = vectors.mapToPair(v -> new Tuple2<>(model.predict(v), v));

        predictions
                .groupByKey()
                .map(KmeansPredict::tuple2string)
                .saveAsTextFile(LocalFiles.firsColumnPredictions);
//        final Vector[] centers = model.clusterCenters();
//        final Map<Integer, Iterable<Vector>> results = predictions.groupByKey().collectAsMap();
//        for (Integer center : results.keySet()) {
//            System.out.println(centers[center].apply(0));
//            for (Vector value : results.get(center)) {
//                System.out.println("\t"+value.apply(0));
//            }
//        }
        jsc.close();
    }

    private static String tuple2string(Tuple2<Integer, Iterable<Vector>> tuple) {
        return tuple._1()+"|~|"+
        StreamSupport
                .stream(tuple._2().spliterator(), true)
                .map(vector -> vector.apply(0))
                .map(Objects::toString)
                .collect(Collectors.joining(","));

    }
}
