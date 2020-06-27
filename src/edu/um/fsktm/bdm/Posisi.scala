package edu.um.fsktm.bdm

import com.jsoniter.JsonIterator


object Posisi {
  def main (args: Array[String]) = {
    println ("Posisi")
    println (Posisi("""{"trj_id": "1047", "driving_mode":"car","osname":"android", "pingtimestamp": 1555732685,"rawlat":1.3143979, "rawlng": 103.8599373 ,"speed":7.5, "bearing":180, "accuracy": 11.0}"""))
  }
  def apply(jsonString: String): Posisi = {
    val any = JsonIterator.deserialize(jsonString);
    Posisi(any.get("trj_id").toString,any.get("driving_mode").toString,any.get("osname").toString,any.get("pingtimestamp").toLong,any.get("rawlat").toDouble(),
        any.get("rawlng").toDouble(),any.get("speed").toDouble(),any.get("bearing").toLong(),any.get("accuracy").toDouble())
  }
  
}
case class Posisi(trj_id: String, driving_mode: String, osname: String, pingtimestamp: Long,
                  rawlat: Double, rawlng: Double, speed: Double, bearing: Long, accuracy: Double) {
  def toJson = s"""{"trj_id": "${trj_id}", "driving_mode":"${driving_mode}","osname":"${osname}", "pingtimestamp": ${pingtimestamp},"rawlat":${rawlat}, "rawlng": ${rawlng} , "speed":${speed}, "bearing":${bearing}, "accuracy": ${accuracy}}"""
  def toInsertValues = s"""('${trj_id}', '${driving_mode}', '${osname}', ${pingtimestamp}, ${rawlat}, ${rawlng},${speed}, ${bearing}, ${accuracy})"""
}

