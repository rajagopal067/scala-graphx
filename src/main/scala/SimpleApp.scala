import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object SimpleApp {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("graphx Application").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val users: RDD[(VertexId, (String, String))] =
        sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
          (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
      // Create an RDD for edges
      val relationships: RDD[Edge[String]] =
        sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
          Edge(2L, 5L, "colleague"), Edge(5L, 7L, "advisor")))
      // Define a default user in case there are relationships with missing user
      val defaultUser = ("John Doe", "Missing")
      // Build the initial Graph
      val graph = Graph(users, relationships)
      graph.cache()

      val facts: RDD[String] =
        graph.triplets.map(triplet =>
          triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
        facts.collect.foreach(println(_))

      val graph1 = graph.subgraph(epred = triplet => triplet.attr != "advisor")

      val facts1: RDD[String] =
        graph1.triplets.map(triplet =>
          triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
      facts1.collect.foreach(println(_))

//      val graph2 = graph.subgraph(epred = triplet => triplet.attr != "collab")
//      val facts2: RDD[String] =
//        graph2.triplets.map(triplet =>
//          triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
//      facts2.collect.foreach(println(_))




    }

}