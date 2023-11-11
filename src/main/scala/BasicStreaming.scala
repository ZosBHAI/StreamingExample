import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
//Part 1
object BasicStreaming extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("firstStreaming")

  val ssc = new StreamingContext(conf,Seconds(2))

  val Topics = Array("kafkaspark")
  val kafkaParms= Map[String,Object](
  "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],"value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafka_demo_group",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)

  )

  val streams = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](
    Topics,kafkaParms
  ))
  streams.foreachRDD(rdd =>{
    val lines = rdd.map(_.value())
    val words = lines.flatMap(x => x.split(" "))
    val wordscount = words.map(x => (x,1L)).reduceByKey(_+_)
    wordscount.foreach(println)
  })

}
