import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
object exactlyOnce  extends App{

  // Aggregate the error log to MYSQL  table
  //Exercise o: find some other source for final count storage

  //Exactly-once with Transactional Writes
  /*
  * Kafkadevelop Workspace
  * One can generate from batch time, partition id, or Kafka offsets,
  * and then write the result along with the
  *  identifier into external storage within a single transaction.
  * foreachPartition - usually it is used  when using with Database;
  * It is true for  map-only procedure, because
  *  Kafka RDD’s partition is correspondent to Kafka partition,
  * so we can extract each partition’s offset like this:
   */
  /*
  * insert ignore is not needed ;  create mysql table
  * for storing the kafka topic,partition,offset
  * where topic + partition is the key
   */
  case class Log(time: LocalDateTime, level: String)

  //val logPattern = "^(.{19}) ([A-Z]+).*".r   //this is  has space between 2 re ; but  we need tab
  val logPattern = "^(.{19})\\s([A-Z]+).*".r  // this one account for the  tab
  val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  def parseLog(line: String): Option[Log] = {
    //println("inside Parse logs" + line)
    line match {
      case logPattern(timeString, level) => {
         // println("Pattern Matched" + timeString)
        val timeOption = try {
          Some(LocalDateTime.parse(timeString, dateTimeFormatter))
        } catch {
          case _: DateTimeParseException => None
        }
        timeOption.map(Log(_, level))
      }
      case _ => {
        None
      }
    }
  }
  def processLogs(messages: RDD[ConsumerRecord[String, String]]): RDD[(LocalDateTime, Int)] = {
     messages.map(_.value)
      .flatMap(parseLog)
      .filter(_.level == "ERROR")
      .map(log => log.time.truncatedTo(ChronoUnit.MINUTES) -> 1)
      .reduceByKey(_ + _)
  }
  val brokers = "localhost:9092"
  val topic = "kafkaspark"
  val kafkaParams = Map[String,Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "exactly-once",
    "enable.auto.commit" -> (false: java.lang.Boolean),
    "auto.offset.reset" -> "none")  // if te commit is not performed ; from  where has the data be read

  ConnectionPool.singleton("jdbc:mysql://127.0.0.1:3306/mydb", "root", "godgodgod59")

  val sparkConf = new SparkConf().setAppName("exactlyOnce").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  ssc.sparkContext.setLogLevel("ERROR")
  val fromOffsets = DB.readOnly { implicit session =>
    sql"""
      select `partition`, offset from kafka_offset
      where topic = ${topic}
      """.map { rs =>
      new TopicPartition(topic, rs.int("partition")) -> rs.long("offset")
    }.list.apply().toMap
  }
  val keyList = fromOffsets.keys
  println("Offset keys " )
  keyList.foreach(println)
  //keyList.
  val messages = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
    ConsumerStrategies.Assign[String,String](fromOffsets.keys, kafkaParams, fromOffsets))

  messages.foreachRDD { rdd =>
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
   // val result = rdd.map(_.value())
   //val result = processLogs(rdd)
   val result = processLogs(rdd).collect()
    /*val result = rdd.map(_.value)
     .flatMap(parseLog)*/
      // .filter(_.level == "ERROR")
     // .map(log => log.time.truncatedTo(ChronoUnit.MINUTES) -> 1)
     // .reduceByKey(_ + _).collect()
    println("Printing the result content")
    //result.foreach(println)
    //val result = processLogs(rdd)


    DB.localTx { implicit session =>
      result.foreach { case (time, count) =>
        sql"""
          insert into error_log (log_time, log_count)
          value (${time}, ${count})
          on duplicate key update log_count = log_count + values(log_count)
          """.update.apply()
      }

      offsetRanges.foreach { offsetRange =>
       // println("Insert Ignore the offset and partition" + s"${offsetRange.partition} "+ " " + s"${offsetRange.fromOffset}")
        //think the below query is used whenever a new Kafka topic + partiton is dynamically created
        // The insert ignore is not  needed ; first we will create a topic + partition as primary key in the  relational
        // table
       /* sql"""
          insert ignore into kafka_offset (topic, `partition`, offset)
          value (${topic}, ${offsetRange.partition}, ${offsetRange.fromOffset})
          """.update.apply()*/
        println("Before update " )
        println(s"${topic}" + "    " + s"${offsetRange.untilOffset}" + "  " + s"${offsetRange.fromOffset}" + "Partition " + s"${offsetRange.partition}")
        val affectedRows = sql"""
          update kafka_offset set offset = ${offsetRange.untilOffset}
          where topic = ${topic} and `partition` = ${offsetRange.partition}
          and offset = ${offsetRange.fromOffset}
          """.update.apply()
        println("affected Rows is " + affectedRows)
        if (affectedRows != 1) {
          throw new Exception("fail to update offset")
        }
      }
    }
  }

  ssc.start()
  ssc.awaitTermination()
}


