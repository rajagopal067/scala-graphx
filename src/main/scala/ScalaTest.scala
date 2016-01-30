/**
  * Created by rajagopal on 1/28/16.
  */

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("graphx Application")
    val sc = new SparkContext(conf)
    println("hello")
  }

}
