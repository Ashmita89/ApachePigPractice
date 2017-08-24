import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkWordCount{
  def main(args: Array[String]) {
    val logFile = "C:/spark-1.6.1-bin-hadoop2.6/README.md" 
    val conf = new SparkConf().setAppName("Spark WordCount")
    val sc = new SparkContext(conf)
	val textFile = sc.textFile(logFile,2)
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    System.out.println(counts.collect().mkString(", "))
	
  }
}