package sdata.pre.process

class ExtractFirstColumn {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    System.setProperty("log4j.configuration", "log4j.properties")
    SparkSession.builder
      .appName(classOf[ExtractFirstColumn].getSimpleName)
//      .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.properties")
      .master("local[*]")
  }

}
