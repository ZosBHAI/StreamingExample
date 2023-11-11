import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

//Part-3
// Spark Streaming  Example - for storing the offset at Kafka itself (Suitable on;y for idempotent operation
/*
Code Ref:https://github.com/koeninger/kafka-exactly-once/blob/master/src/main/scala/example/CommitAsync.scala
Adv- Unlike checkpointing, it is not affected by application code change
 Disadv :  There may be duplicates as Kafka is not transcational(i.e Kafka is atleast once or atmost once delivery )
*/




object CommitAsyncDirectkafkaStreaming extends App {
 // println("THis is the begining")
  //val spark = SparkSession.builder().appName("DStreamKafka").master("local[*]").getOrCreate()

  val sparkConf = new SparkConf().setAppName("DirectStream").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf,Seconds(2))
  //StreamingContext.getOrCreate()
  ssc.sparkContext.setLogLevel("ERROR")
  //ssc.checkpoint("C:\\hadoop")
  val topicSet = Array("kafkaspark")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer], //this is mentoned as the producer serialse the key and Value
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafka_demo_group",
    "auto.offset.reset" -> "earliest",  //read it from the  last commit
    "enable.auto.commit" -> (false: java.lang.Boolean) //kakfka dstream will commit consumer offset once it receives.
                  // disable the commit option for successful data sent
  )
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topicSet, kafkaParams)
  )

  //all the records are received  in the form of an object of class ConsumerRecords
  stream.foreachRDD { rdd =>
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //get the range of offset from last commited from Kafka
    val lines = rdd.map(_.value())  //process the messages in that range
    val words = lines.flatMap(_.split(" "))
    val wordscount = words.map(x => (x,1L)).reduceByKey(_+_)
    //println(" RDD printing")
    wordscount.foreach(println)
   stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges) //asynchronously commiting after processing @ Kafka side
  }
  //val lines = stream.map(_.value())
  //stream.print()
  ///al words = lines.flatMap(_.split(" "))
  //val wordscount = words.map(x => (x,1L)).reduceByKey(_+_)
  //wordscount.print()

  //start  the Streaming Application
  ssc.start()
  ssc.awaitTermination()



}
