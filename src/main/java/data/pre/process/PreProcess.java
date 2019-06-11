package data.pre.process;

import org.apache.spark.api.java.JavaSparkContext;

public class PreProcess {
    public static void main(String[] args) {
        final JavaSparkContext jsc = new JavaSparkContext("local[*]", "PreProcess");
        ExtractFirstColumn.readWrite(jsc);
        FilterAbnormalities.readWrite(jsc);
        jsc.close();
    }
}
