package foo.bar.baz 

import java.sql.{Date, Timestamp}

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object WordCountExample {

  val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  def run(spark: SparkSession, kafkaBroker: String, kafkaTopicIn: String, kafkaTopicOut: String): Unit = {
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val inputSchema: StructType = new StructType().add("ts", TimestampType).add("str", StringType)

    val df:DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", kafkaTopicIn)
      .load()

    val data: Dataset[(String, Timestamp)] = df
      .select(col("value").cast("string"))
      .select(from_json(col("value"), inputSchema).as("jsonConverted"))
      .select("jsonConverted.str", "jsonConverted.ts").as[(String, Timestamp)]

    val wordFrequencies: DataFrame = data
      .filter(col("str").isNotNull)
      .withColumn("word", explode(split($"str", " ")))
      .withWatermark("ts", "5 seconds")
      .groupBy(window(col("ts"), "15 seconds", "5 seconds"), col("word"))
      .agg(count($"word").as("frequency"))
      .select("word", "frequency", "window")
    
    // write to console
    // val query: StreamingQuery = wordFrequencies
    //   .orderBy(desc("frequency"), desc("window.start"))
    //   .writeStream
    //   .format("console")
    //   .outputMode(OutputMode.Complete())
    //   .option("truncate", "false")
    //   .start()

    val query: StreamingQuery = wordFrequencies
      .orderBy(desc("frequency"), desc("window.start"))
      .withColumn("value", to_json(struct(wordFrequencies.columns.map(col(_)):_*)))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", kafkaTopicOut)
      .option("checkpointLocation", "tmp/kbduvakin/check")
      .outputMode(OutputMode.Complete())
      .start()

    query.awaitTermination()
  }

  def main(args:Array[String]): Unit = {
    if(args.length != 3) {
      println("Invalid usage")
      println("Usage: spark-submit <.jar> <kafka_broker> <kafka_topic_in> <kafka_topic_out>")
      LOG.error(s"Invalid number of arguments, arguments given: [${args.mkString(",")}]")
      System.exit(1)
    }
    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark-streaming-kafka")
      .getOrCreate()

    run(spark, args(0), args(1), args(2))
  }

}