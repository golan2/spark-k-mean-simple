package data.pre.process;

import data.common.LocalFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ExtractFirstColumn {


    public static void main(String[] args) {
        final JavaSparkContext jsc = new JavaSparkContext("local", "Anomaly Detection");
        final JavaRDD<String> raw = jsc.textFile(LocalFiles.kddDataPath);
        final JavaRDD<String> firstColumn = raw.map(ExtractFirstColumn::getFirstColumn);
        firstColumn.repartition(1).saveAsTextFile(LocalFiles.firstColumnDataPath);
        jsc.close();
    }

    private static String getFirstColumn(String line) {
        int index = -1;
        for (int i=0 ; i<4 ; i++) {
            index = line.indexOf(",", index+1);
        }
        return line.substring(index+1, line.indexOf(",", index+1));
    }
}
