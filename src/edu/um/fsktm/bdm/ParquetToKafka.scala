package edu.um.fsktm.bdm

import com.google.common.io.Files
import scala.util.Random
import com.github.mjakubowski84.parquet4s.ParquetReader
import java.io.File
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerRecord

object ParquetToKafka {
  val dataPath = "D:\\Lecture\\Semester 4\\WQD7007 Big Data Management\\Project\\GrabData\\posisi\\city=Singapore"
  val props = new Properties()
  props.put("bootstrap.servers", "192.168.56.103:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC = "posisi"

  def main(args: Array[String]) = {
    println("ParquetToKafka")
    val files = listFiles(dataPath)
    files.take(3).foreach {
      file => 
        println(s"Processing File ${file}")
        processFile(file.getAbsolutePath)
    }
  }

  def processFile(filePath: String) = {
    val records = readParquet(filePath)
    println(s"Loading ${records.size} from File ${filePath}")
    records.foreach { record =>
      producer.send(new ProducerRecord(TOPIC, record.pingtimestamp.toString(), record.toJson))
    }
  }

  def listFiles(dataPath: String): Array[File] = {
    val d = new File(dataPath)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).sorted
    } else {
      Array[File]()
    }
  }

  def readParquet(filePath: String) = ParquetReader.read[Posisi](filePath)

}

case class Posisi(trj_id: String, driving_mode: String, osname: String, pingtimestamp: Long,
                  rawlat: Double, rawlng: Double, speed: Double, bearing: Long, accuracy: Double) {
  def toJson = s"""{"trj_id": "${trj_id}", "driving_mode":"${driving_mode}","osname":"${osname}", "pingtimestamp": ${pingtimestamp},"rawlat":${rawlat}, "rawlng": ${rawlng} , "speed":${speed}, "bearing":${bearing}, "accuracy": ${accuracy}}"""
}

