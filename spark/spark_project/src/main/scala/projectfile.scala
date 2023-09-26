package foo.bar.baz 
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountExample {

  def main(args:Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("wordcount")
      .getOrCreate()

    val sc = spark.sparkContext
    val rdd = sc.textFile("src/main/resources/test.txt")
    val counts = rdd.repartition(3)
      .flatMap(line => line.split(" ")) 
      .map(word => (word,1))
      .reduceByKey(_+_)
      .map(a => (a._2, a._1))
      .sortByKey(false)

    counts.saveAsTextFile("src/main/resources/result/")
    
  }
}