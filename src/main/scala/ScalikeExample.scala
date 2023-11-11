import scalikejdbc._
object ScalikeExample extends App {
  ConnectionPool.singleton("jdbc:mysql://127.0.0.1:3306/mydb", "root", "godgodgod59")
  DB.localTx { implicit session =>
    val insignore = sql"""
          insert ignore into kafka_offset (topic, `partition`, offset)
          value ("kafkaspark", 0, 1)
          """.update.apply()
    println("Insert ignore " + insignore)
    val affectedRows =
      sql"""
          update kafka_offset set offset = 41
          where topic = "kafkaspark" and `partition` = 0
          and offset = 1
          """.update.apply()

    println("Aftected rows " + affectedRows)
    if (affectedRows != 1) {
      throw new Exception("fail to update offset")
    }
  }
}
