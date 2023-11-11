import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//Part 2
object CheckPointingDirectKS   {

  // Unable tp set the checkpoint intreval
  def createContext (checkpointDirectory: String): StreamingContext= {
    println("Creating new context")
    val sparkConf = new SparkConf().setAppName("CheckPointing").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(checkpointDirectory)
    ssc.sparkContext.setLogLevel("ERROR")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka_demo_group",
      "enable.auto.commit" -> (false: java.lang.Boolean) //disable the commit option for successful data sent
    )
    val topicSet = Array("kafkaspark")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicSet, kafkaParams)
    )

    messages.foreachRDD { rdd =>

      val lines = rdd.map(_.value().toString)
      val words = lines.flatMap(_.split(" "))
      val wordcount = words.map(x => (x, 1L)).reduceByKey(_ + _)
      wordcount.foreach(println)

    }
    ssc
  }

  def main(args: Array[String]): Unit = {
    //Create a Streaming Context  based on teh checkpoint directory, If checkpoint data exists in the
    // checkpoint directory, a StreamingContext will be recreated from the checkPOint directory and otherwise
    //new Streaming Context will be created
      val ssc = StreamingContext.getOrCreate("C:\\hadoop\\SparkStreamingChkPoint", () => createContext("C:\\hadoop\\SparkStreamingChkPoint"))
      ssc.start()
      ssc.awaitTermination()
    }
  }
      //println(" RDD printing")
      //wordscount

      //start  the Streaming Application




