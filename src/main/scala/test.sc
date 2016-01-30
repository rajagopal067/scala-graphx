

import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.util.parsing.json.JSONObject
import org.json4s.jackson.Serialization

var map = collection.mutable.Map[String, Any]()
map = map + ("Offer"->"\"http://dig.isi.edu/ht/data/43797529/offer\"")
implicit val formats = DefaultFormats

val str = Serialization.write(map)
str
val newMap = parse(str)
