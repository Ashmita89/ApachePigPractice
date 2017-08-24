import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object QuickStart_Char_ab{
  def main(args: Array[String]) {
    val logFile = "C:/spark-1.6.1-bin-hadoop2.6/README.md" 
    val conf = new SparkConf().setAppName("QuickStart_Char_ab")
    val sc = new SparkContext(conf)
    
	//Sample example as Quickstart 
	val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    System.out.println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
	}
	}
	