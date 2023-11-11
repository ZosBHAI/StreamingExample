import java.util.Properties

import org.apache.kafka.common.TopicPartition
import scalikejdbc._
//import java.util.Arrays
import java.util
import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.KafkaConsumer
/*
https://stackoverflow.com/questions/35437681/kafka-get-partition-count-for-a-topic?answertab=active#tab-top
https://kafka.apache.org/11/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
To understand Consumer : https://blog.knoldus.com/case-study-to-understand-kafka-consumer-and-its-offsets/
 */
object KafkaConsumerSample extends  App {

  def getLastCommittedOffsets(topic: String): Map[TopicPartition, Long] = {


    //  val topic = "kafkaspark"
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("enable.auto.commit", "false")
    props.put("group.id", "exactly-once")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    //consumer.subscribe(util.Arrays.asList(topic))
    val partitionList = consumer.partitionsFor(topic)
    println("Total number of partitions" + partitionList.size())
    val kafkaNumberOfPartitionForTopic = partitionList.size()
    //var fromOffsets:Map[TopicPartition,Long]
    //val fromOffsets = collection.mutable.Map[TopicPartition,Long]()
    ConnectionPool.singleton("jdbc:mysql://127.0.0.1:3306/mydb", "root", "godgodgod59")
    //def mySQLRead(topic:String): Map[TopicPartition,Long] = {
    var fromOffsets = DB.readOnly { implicit session =>
      sql"""
      select `partition`, offset from kafka_offset
      where topic = ${topic}
      """.map { rs =>
        new TopicPartition(topic, rs.int("partition")) -> rs.long("offset")
      }.list.apply().toMap
    }
    //}
    println(" Total number  of  Partitions in the table " + fromOffsets.keys.size)


    val mysqlNumberOfPartitionsForTopic = fromOffsets.keys.size
    if (mysqlNumberOfPartitionsForTopic == 0) {
      for (partition <- 0 to kafkaNumberOfPartitionForTopic - 1) {
        fromOffsets += (new TopicPartition(topic, partition) -> 0)
      }
    }
    else if (mysqlNumberOfPartitionsForTopic < kafkaNumberOfPartitionForTopic) {
      for (partition <- mysqlNumberOfPartitionsForTopic  to kafkaNumberOfPartitionForTopic - 1) {
        fromOffsets += (new TopicPartition(topic, partition) -> 0)
      }
    }

    println("Elements of fromOffsset" + fromOffsets)
    fromOffsets
  }
  getLastCommittedOffsets("kafkaspark")
}
