package com.mongodb



object GettingStarted {

  def main(args: Array[String]): Unit = {

    /* Create the SparkSession.
     * If config arguments are passed from the command line using --conf,
     * parse args for the values to set.
     */
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkWithMongoDb")
      .config("spark.mongodb.input.uri", "mongodb://172.17.0.2/test.characters")
      .config("spark.mongodb.output.uri", "mongodb://172.17.0.2/test.characters")
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")

    import com.mongodb.spark._
    import com.mongodb.spark.config._
    import org.bson.Document

    val docs = """
      {"name": "Bilbo Baggins", "age": 50}
      {"name": "Gandalf", "age": 1000}
      {"name": "Thorin", "age": 195}
      {"name": "Balin", "age": 178}
      {"name": "Kíli", "age": 77}
      {"name": "Dwalin", "age": 169}
      {"name": "Óin", "age": 167}
      {"name": "Glóin", "age": 158}
      {"name": "Fíli", "age": 82}
      {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
    spark.sparkContext.parallelize(docs.map(Document.parse)).saveToMongoDB()


    val df = MongoSpark.load(spark)  // Uses the SparkSession


    val df2 = sc.loadFromMongoDB() // SparkSession used for configuration
    val df3 = sc.loadFromMongoDB(ReadConfig(
      Map("uri" -> "mongodb://172.17.0.2/test.orders")
    )
    ) // ReadConfig used for configuration

//    val df4 = spark.read.mongo() // SparkSession used for configuration
val df4 = spark.read.format("com.mongodb.spark.sql").load()

    // Set custom options
    import com.mongodb.spark.config._

    val customReadConfig = ReadConfig(Map("readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
//    val df5 = spark.read.mongo(customReadConfig)

    val df6 = spark.read.format("com.mongodb.spark.sql").options(customReadConfig.asOptions).load()

    val ordersReadConfig = ReadConfig(Map("uri" -> "mongodb://172.17.0.2/test.orders"))
    val orders = spark.read.format("com.mongodb.spark.sql").options(ordersReadConfig.asOptions).load()

    df.printSchema()                        // Prints DataFrame schema

    println("-----------------df---------------------- ")
    df.show(5)
    println("-----------------df2---------------------- ")
    println(df2.take(5))   // does not print properly
    println("-----------------df3---------------------- ")
    println(df3.take(5))   // does not print properly
    println("-----------------df4---------------------- ")
    df4.show(5)
    println("-----------------df6---------------------- ")
    df6.show(5)
    println("-----------------orders---------------------- ")
    orders.show(5)

    spark.stop()

  }
}