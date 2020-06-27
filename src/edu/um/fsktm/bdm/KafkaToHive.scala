package edu.um.fsktm.bdm

import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
import java.util

object KafkaToHive {
  def main(args: Array[String]) {
    println(s"Kafka To Hive")
    val connection = connectHive()
    createPosisiTable(connection)
    consumeFromKafka("posisi", connection)
  }

  def connectHive() = {
    val driver = "org.apache.hive.jdbc.HiveDriver";
    val connectionUrl = "jdbc:hive2://192.168.56.103:10000/;ssl=false"
    Class.forName(driver);
    DriverManager.getConnection(connectionUrl, "student", "");
  }

  def createPosisiTable(connection: Connection) = {
    val sqlStatementCreate = "CREATE TABLE posisi (trj_id STRING, driving_mode STRING, osname STRING, pingtimestamp BIGINT, rawlat DOUBLE , rawlng DOUBLE, speed DOUBLE, bearing BIGINT , accuracy DOUBLE ) STORED AS PARQUET"
    val stmt = connection.createStatement();
    stmt.execute(sqlStatementCreate);
  }

  def insertPosisiRow(connection: Connection, datas: Iterable[Posisi]) = {
    val values = datas.map(data => data.toInsertValues).mkString(",")
    def insertStatement(datas: Iterable[Posisi]) = s"""insert into posisi values ${values}"""
    val stmt = connection.createStatement
//    println (insertStatement(datas))
    stmt.executeUpdate(insertStatement(datas))
  }

  def consumeFromKafka(topic: String, connection: Connection) = {
    val props = new Properties()
    props.put("bootstrap.servers", "192.168.56.103:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", "test")
    props.put("max.poll.records" , "5000")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    var start = System.currentTimeMillis()
    while (true) {
      val record = consumer.poll(1000).asScala
      insertPosisiRow (connection, record.map(_.value()).map(Posisi(_))) 
      println (s"processing ${record.size} in ${System.currentTimeMillis() - start} ms")
      start = System.currentTimeMillis()
    }
  }
  
  def test() {
    val connection = connectHive()
    val sqlStatementCreate = "CREATE TABLE helloworld (message String) STORED AS PARQUET";
    val stmt = connection.createStatement();
    stmt.execute(sqlStatementCreate);
    
    val sqlStatementInsert = """INSERT INTO helloworld VALUES ('helloworld')""";
    stmt.execute(sqlStatementInsert);
    
    val sqlStatementSelect = "SELECT * from helloworld";
    val rs = stmt.executeQuery(sqlStatementSelect);
    // Process results
    while(rs.next()) {
       println(rs.getString(1));
    }
  }
}