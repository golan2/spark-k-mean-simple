package dataflow.preprocess


import org.apache.spark.sql.Encoder
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import scala.reflect.ClassTag


object PairWithSpaces {

  def main(args: Array[String]): Unit = {
    val file = "/Users/ig2258/Desktop/s1.txt"

    val session = SparkSession.builder().master("local[*]").appName("PairWithSpaces").getOrCreate()

    import session.implicits._

    val ds = session.read
      .format("text")
      .load(file)
      .as[String]
      .map(s => s.trim)
      .map(s => s.split(" +").map(_.toDouble))
      .map(a =>Vectors.dense(a))(new EE)


    println("~~~~~"+ds.take(10).mkString("\n"))

    val model = new KMeans().setK(10).fit(ds)

    println(s"MODEL: \n${model.clusterCenters.mkString("\n")}")

  }

  class EE extends Encoder[org.apache.spark.ml.linalg.Vector] {
    override def schema: StructType = new StructType() {

    }

    override def clsTag: ClassTag[linalg.Vector] = ???
  }
}


