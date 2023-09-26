package foo.bar.baz

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

import java.util
import org.apache.log4j.Logger



object streamKafka {
    val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

    def run(groupId: String, kafkaBroker: String, topicIn: String, topicOut: String): Unit = {
        
        val sparkConf = new SparkConf()
            .setAppName("streamKafkaApplication")
            .setMaster("local[3]")
            .set("spark.executor.memory", "1g")
        
        val ssc = new StreamingContext(sparkConf, Seconds(2))
        ssc.checkpoint("tmp/check")

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> kafkaBroker,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        val topic = Array(topicIn)
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topic, kafkaParams)
        )
        val keyValuePairs = stream.map(record => (record.key, record.value))
        val lines = keyValuePairs.map(_._2)   
        val words = lines.flatMap(_.split(" "))
        val wordPairs = words.map(word => (word, 1L))
        val windowedWordCounts = wordPairs
            .reduceByKeyAndWindow(
                _ + _,
                _ - _,
                Seconds(15), 
                Seconds(5)
            )
        
        val props = new util.HashMap[String, Object]()
        props.put("bootstrap.servers", kafkaBroker)
        props.put("key.serializer", classOf[StringSerializer])
        props.put("value.serializer", classOf[StringSerializer])
        
        windowedWordCounts.foreachRDD { rdd =>
            rdd.foreachPartition {partitionOfRecords =>
                val producer = new KafkaProducer[String, String](props)
                partitionOfRecords.foreach { message =>
                    val prodRecord = new ProducerRecord[String, String](topicOut, message.productIterator.mkString("\t"))
                    producer.send(prodRecord)
                }
                producer.close()
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }

    def main(args:Array[String]): Unit = {
        if(args.length != 4) {
            println("Invalid usage")
            println("Usage: spark-submit <.jar> <kafka_group_id> <kafka_broker> <kafka_topic_in> <kafka_topic_out>")
            LOG.error(s"Invalid number of arguments, arguments given: [${args.mkString(",")}]")
            System.exit(1)
        }
        run(args(0), args(1), args(2), args(3))
    }
}