package k.means;

import data.common.LocalFiles;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// This is the code as was taken from here
// http://www.stepbystepcoder.com/using-spark-for-anomaly-fraud-detection-k-means-clustering/
// hence we suppress the following warnings because I want the code "as is"
@SuppressWarnings({"Convert2Lambda", "RedundantThrows", "Convert2Diamond", "RedundantIfStatement"})
public class AnomalyApp {

    private static final String kddDataPath = LocalFiles.kddDataTenPercent;


    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "AnomalyApp");

        JavaRDD<Vector> kddRDD = jsc
                .textFile(kddDataPath)
                .map(new KddParser())
                .cache();


        System.out.println("KDD data row size : " + kddRDD.count());
        System.out.println("Example data : " + kddRDD.first());


        JavaDoubleRDD firstColumn = kddRDD.mapToDouble((DoubleFunction<Vector>) t -> t.apply(0));

        final double mean = firstColumn.mean();
        final double stdev = firstColumn.stdev();

        System.out.println("Meaning value : " + mean + " Standard deviation : " + stdev + " Max : " + firstColumn.max() + " Min : " + firstColumn.min());


        JavaRDD<Vector> filteredKddRDD = kddRDD
                .filter(new OutlierFilter(firstColumn.mean(), firstColumn.stdev()))
                .cache();


        System.out.println("Filtered data ...  Count : " + filteredKddRDD.count());
        System.out.println("Example data : " + filteredKddRDD.first());

        final int numClusters = 10;
        final int numIterations = 20;

        System.out.println("K means clusters: " + numClusters);
        System.out.println("K means iterations: " + numIterations);

        final KMeansModel clusters = KMeans.train(filteredKddRDD.rdd(), numClusters, numIterations);


        Vector[] clusterCenters = clusters.clusterCenters();
        System.out.println("Centers:");
        System.out.println(Arrays.stream(clusterCenters).map(Vector::toJson).collect(Collectors.joining(",\n ", "  ", "")));

        // ========================


        JavaPairRDD<Vector, Double> predictions = kddRDD.mapToPair(new Predictor(clusters));

        final Map<Vector, Long> itemsPerModel = predictions.countByKey();
        System.out.println("countByKey:");
        itemsPerModel.forEach((v,n) -> System.out.println(" - ["+n+"] => ["+v.toJson()+"]"));

        // ========================

        List<Tuple2<Vector, Double>> benTen = predictions.take(10);

        //Print top ten points
        for(Tuple2<Vector, Double> tuple : benTen){
            System.out.println("Distance " + tuple._2());
        }


        // ========================


        final JavaPairRDD<Vector, Double> party = predictions.partitionBy(new ByClusterPartitioner(clusterCenters));

        party.foreachPartition(new VoidFunction<Iterator<Tuple2<Vector, Double>>>() {
            @Override
            public void call(Iterator<Tuple2<Vector, Double>> it) throws Exception {

            }
        });


    }

    private static class KddParser implements Function<String, Vector>{


        @Override
        public Vector call(String line) throws Exception {
            String[] kddArr = line.split(",");

            double[] values = new double[37];
            for (int i = 0; i < 37; i++) {
                values[i] = Double.parseDouble(kddArr[i + 4]);
            }
            return Vectors.dense(values);
        }




    }
    private static class OutlierFilter implements Function<Vector, Boolean> {

        final double mean;
        final double stdev;

        OutlierFilter(double mean, double stdev) {
            this.mean = mean;
            this.stdev = stdev;
        }

        @Override
        public Boolean call(Vector v1) throws Exception {
            double src_bytes = v1.apply(0);
            if (src_bytes > (mean - 2 * stdev) && src_bytes < (mean + 2 * stdev)) {
                return true;
            }
            return false;
        }

    }

    private static class Predictor implements PairFunction<Vector, Vector, Double> {

        private final KMeansModel clusters;
        private final Vector[] clusterCenters;
        private Predictor(KMeansModel clusters) {
            this.clusters = clusters;
            this.clusterCenters = clusters.clusterCenters();
        }

        @Override
        public Tuple2<Vector, Double> call(Vector point) throws Exception {
            int centroidIndex = clusters.predict(point);  //find centroid index
            Vector centroid = clusterCenters[centroidIndex]; //get cluster center (centroid) for given point
            //calculate distance
            double preDis = 0;
            for(int i = 0 ; i < centroid.size() ; i ++){
                preDis = Math.pow((centroid.apply(i) - point.apply(i)), 2);

            }
            double distance = Math.sqrt(preDis);
            return new Tuple2<Vector, Double>(point, distance);
        }


    }

    private static class ByClusterPartitioner extends Partitioner {
        private final int N_PULS_ONE;
        private final Map<Vector, Integer> clusterCenters;

        private ByClusterPartitioner(Vector[] clusterCenters) {
            this.clusterCenters = new HashMap<>();
            for (int i = 0; i < clusterCenters.length; i++) {
                this.clusterCenters.put(clusterCenters[i], i);
            }
            this.N_PULS_ONE = this.clusterCenters.size()+1;
        }

        @Override
        public int numPartitions() {
            return this.clusterCenters.size()+1;
        }

        @Override
        public int getPartition(Object key) {
            if (key instanceof Tuple2) {
                final Tuple2<Vector, Double> k = (Tuple2<Vector, Double>) key;
                final Integer partition = this.clusterCenters.get(k._1);
                if ( partition!=null ) return partition;
            }
            return N_PULS_ONE;
        }
    }
}