package edu.um.fsktm.bdm

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin

object KafkaToHbase {
  def main(args: Array[String]) {
    println(s"Kafka To Hbase")
    test()
//    val connection = connectHBase()
//    createPosisiTable(connection)
//    consumeFromKafka("posisi", connection)
  }

  def connectHBase() = {
    val conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "192.168.56.103");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
//    conf.set("zookeeper.znode.parent", "/hbase");
//    conf.set("hbase.master", "192.168.56.103:60000");
    val con = ConnectionFactory.createConnection(conf);
    HBaseAdmin.checkHBaseAvailable(conf);
    println ("Connected")
    con
  }

//  def createPosisiTable(connection: Connection) = {
//    val sqlStatementCreate = "CREATE TABLE posisi (trj_id STRING, driving_mode STRING, osname STRING, pingtimestamp BIGINT, rawlat DOUBLE , rawlng DOUBLE, speed DOUBLE, bearing BIGINT , accuracy DOUBLE ) STORED AS PARQUET"
//    val stmt = connection.createStatement();
//    stmt.execute(sqlStatementCreate);
//  }
//
//  def insertPosisiRow(connection: Connection, datas: Iterable[Posisi]) = {
//    val values = datas.map(data => data.toInsertValues).mkString(",")
//    def insertStatement(datas: Iterable[Posisi]) = s"""insert into posisi values ${values}"""
//    val stmt = connection.createStatement
////    println (insertStatement(datas))
//    stmt.executeUpdate(insertStatement(datas))
//  }
//
//  def consumeFromKafka(topic: String, connection: Connection) = {
//    val props = new Properties()
//    props.put("bootstrap.servers", "192.168.56.103:9092")
//    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put("auto.offset.reset", "earliest")
//    props.put("group.id", "test")
//    props.put("max.poll.records" , "5000")
//    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
//    consumer.subscribe(util.Arrays.asList(topic))
//    var start = System.currentTimeMillis()
//    while (true) {
//      val record = consumer.poll(1000).asScala
//      insertPosisiRow (connection, record.map(_.value()).map(Posisi(_))) 
//      println (s"processing ${record.size} in ${System.currentTimeMillis() - start} ms")
//      start = System.currentTimeMillis()
//    }
//  }
//  
  def test() {
    val connection = connectHBase()

//    val admin = connection.getAdmin();
//
//    val tableDesc = new HTableDescriptor("test");
//    tableDesc.addFamily(new HColumnDescriptor("family"));
//    admin.createTable(tableDesc);

    val table = connection.getTable(TableName.valueOf("test"));

    val put1 = new Put(Bytes.toBytes("row1"));
    val put2 = new Put(Bytes.toBytes("row2"));
    val put3 = new Put(Bytes.toBytes("row3"));
//    put1.addImmutable(family1.getBytes(), qualifier1, Bytes.toBytes("cell_data"));
//    table.put(put1);

    put1.addColumn(Bytes.toBytes("posisi"), Bytes.toBytes("qual1"),
      Bytes.toBytes("ValueOneForPut1Qual1"));
    put2.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual1"),
      Bytes.toBytes("ValueOneForPut2Qual1"));
    put3.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual1"),
      Bytes.toBytes("ValueOneForPut2Qual1"));
    put1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual2"),
      Bytes.toBytes("ValueOneForPut1Qual2"));
    put2.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual2"),
      Bytes.toBytes("ValueOneForPut2Qual2"));
    put3.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual2"),
      Bytes.toBytes("ValueOneForPut3Qual3"));
    table.put(put1);
    table.put(put2);
    table.put(put3);

  }
}