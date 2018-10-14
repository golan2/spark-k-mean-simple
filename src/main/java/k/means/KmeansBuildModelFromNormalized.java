package k.means;

import data.common.LocalFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class KmeansBuildModelFromNormalized {

    public static void main(String[] args) {
        final JavaSparkContext jsc = new JavaSparkContext("local", "Anomaly Detection");
        final JavaRDD<Vector> norma = jsc.textFile(LocalFiles.firstColumnNormalized).map(s -> Vectors.dense(Double.parseDouble(s)));
        final KMeansModel model = KMeans.train(norma.rdd(), 10, 20);
        model.save(jsc.sc(), LocalFiles.firstColumnModel);
        jsc.close();
    }

}
