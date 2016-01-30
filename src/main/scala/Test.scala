/**
  * Created by rajagopal on 1/21/16.
  */

import org.apache.hadoop.io.Text
import org.apache.spark.graphx.{Edge, VertexId, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._


object Test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("graphx Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val filePath = "/Users/rajagopal/Desktop/github_repos/dig-graphx/sample-files/seq-files/Offer"
    val filePath = args(0)
    val offerRDD = sc.sequenceFile(filePath,classOf[Text],classOf[Text])

    // create vertexRDDs and edgeRDDs in the format required
    val helper = new GraphHelper

    val vertexRDD : RDD[(VertexId,String)]= offerRDD.flatMap(line => helper.vertex_mapper(line._1,line._2))
    vertexRDD.foreach(line => println(line))

    val edgeRDD : RDD[Edge[String]] = offerRDD.flatMap(line => helper.edge_mapper(line._1,line._2))

    edgeRDD.foreach(line => println(line))

//    vertexRDD.saveAsTextFile(args(1))
//    edgeRDD.saveAsTextFile(args(2))


    // create graph and compute connected components
    val graph = Graph(vertexRDD,edgeRDD)

//    explore how subgraph works and how to select edges  ???

    val sellerGraph = graph.subgraph(vpred = (id,str) => filterSellerOffer(id,str))
      .subgraph(epred = triplet => triplet.attr == "seller")


//    sellerGraph.vertices.foreach(line => println(line._2))

    val cc = sellerGraph.connectedComponents().vertices
//
    val rev = cc.map{
      case (u,v) => (v,u)
    }
    val grouped = rev.groupByKey()
    grouped.foreach(line => line._2.size > 2)
//    grouped.foreach((id) => println(id._1 + " is connected to " + id._2))
//    grouped.saveAsTextFile("sellergraph")

//    val tripletRDD: RDD[String] =
//      sellerGraph.triplets.map(triplet =>
//        triplet.dstAttr + " is the " + triplet.attr + " of " + triplet.srcAttr)
//
//    tripletRDD.collect.foreach(println(_))


  }

  def filterSellerOffer(id : VertexId,str : String) : Boolean = {
    val JSON = parse(str)
    val vertex_type = compact(JSON\"type")
    (vertex_type=="\"Seller\"" || vertex_type=="\"Offer\"")

  }

}
