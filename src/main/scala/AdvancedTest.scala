import org.apache.spark.graphx.GraphLoader
import org.apache.hadoop.io.Text
import org.apache.spark.graphx.{Edge, VertexId, Graph}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by rajagopal on 1/28/16.
  */
object AdvancedTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("graphx Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, "/Users/rajagopal/Desktop/followers.txt")
    // Find the connected components
    val cc = graph.connectedComponents().vertices
    val rev = cc.map{
      case (u,v) => (v,u)
    }
    val grouped = rev.groupByKey()
    grouped.foreach((id) => println(id._1 + " is connected to " + id._2))


    // Join the connected components with the usernames
    val users = sc.textFile("/Users/rajagopal/Desktop/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }

    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (cc, username)
    }

    val groupedUsers = ccByUsername.groupByKey().map{
      case (id,name) => (id,name)
    }

    groupedUsers.foreach(line => println())


//     Print the result
//    println(ccByUsername.collect().mkString("\n"))


  }

}
