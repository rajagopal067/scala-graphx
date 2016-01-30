import org.apache.spark.graphx.{Graph, VertexId, Edge}
import org.apache.spark.{SparkContext, SparkConf}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.io.Text
import org.json4s.jackson.Serialization

import scala.util.hashing.Hashing


/**
  * Created by rajagopal on 1/21/16.
  */
class GraphHelper() extends Serializable{

  def vertex_mapper(key : Text,value:Text):Array[(VertexId, String)] = {
    val JSON = parse(value.toString)
    //if 'a' is offer return json line with added type of Offer
    var map = collection.mutable.Map[String, Any]()
    implicit val formats = DefaultFormats

    val arr = new Array[(VertexId,String)](5)

    val offer_uri = compact(JSON\"uri")
    //adding the type of object and uri of the node
    map = map + ("uri"->offer_uri) + ("type"->"Offer")
    arr(0) = (hash(offer_uri),Serialization.write(map))

    //if 'availableAtorfrom' is present and 'a' is Place return json line with added type of Place
    val place_uri = compact(JSON\"availableAtOrFrom"\"uri")
    map = map + ("uri"->place_uri) + ("type"->"Place")
    arr(1) = (hash(place_uri),Serialization.write(map))

    //if itemOffered is there return type AdultService
    val as_uri = compact(JSON\"itemOffered")
    map = map + ("uri"->as_uri) + ("type"->"AdultService")
    arr(2) = (hash(as_uri),Serialization.write(map))

    //if mainEntity is present and return type WebPage
    val wp_uri = compact(JSON\"mainEntityOfPage")
    map = map + ("uri"->wp_uri) + ("type"->"WebPage")
    arr(3) = (hash(wp_uri),Serialization.write(map))

    //seller is present return type "Seller"
    val seller_uri = compact(JSON\"seller")
    map = map + ("uri"->seller_uri) + ("type"->"Seller")
    arr(4) = (hash(seller_uri),Serialization.write(map))
    arr
  }


  def edge_mapper(key : Text,value: Text) : Array[Edge[String]] = {

    val JSON = parse(value.toString)
    //if 'a' is offer return json line with added type of Offer
    val arr = new Array[Edge[String]](4)
    var map = collection.mutable.Map[String, Any]()
    implicit val formats = DefaultFormats

    val offer_uri = compact(JSON\"uri")
    //if 'availableAtorfrom' is present and 'a' is Place return json line with added type of Place
    val place_uri = compact(JSON\"availableAtOrFrom"\"uri")

    arr(0) = Edge(hash(offer_uri),hash(place_uri),"availableAtOrFrom")

    //if itemOffered is there return type AdultService
    val as_uri = compact(JSON\"itemOffered")
    arr(1) = Edge(hash(offer_uri),hash(as_uri),"itemOffered")

    //if mainEntity is present and return type WebPage
    val wp_uri = compact(JSON\"mainEntityOfPage")
    arr(2) = Edge(hash(offer_uri),hash(wp_uri),"mainEntityOfPage")

    //seller is present return type "Seller"
    val seller_uri = compact(JSON\"seller")
    arr(3) = Edge(hash(offer_uri),hash(seller_uri),"seller")
    arr

  }

  def hash(str:String):Long={
    var hash = 7
    var i=0
    for(i<- 0 until str.length-1) {
      hash = hash * 31 + str.charAt(i)
    }
    math.abs(hash % 982451653)
  }

}
