package k.means;

import data.common.LocalFiles;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;

public class KmeansBuildModelFromNormalized {

    public static void main(String[] args) {
        final JavaSparkContext jsc = new JavaSparkContext("local", "Anomaly Detection");
        final RDD<Vector> norma = jsc
                .textFile(LocalFiles.firstColumnNormalized)
                .map(Double::parseDouble)
                .map(Vectors::dense)
                .rdd();
        final KMeansModel model = KMeans.train(norma, 10, 20);
        model.save(jsc.sc(), LocalFiles.firstColumnModel);
        jsc.close();
    }

}
