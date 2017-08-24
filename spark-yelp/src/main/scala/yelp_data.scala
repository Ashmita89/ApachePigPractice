import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


object yelp_data{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Yelp Business").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)
	import sqlContext.implicits._
	val yelp_business_file = "C:/Users/Ashmita/Documents/BigData/Yelp/ExtractedDataset/yelp_academic_dataset_business.json" 
	val yelp_business = sqlContext.read.json(yelp_business_file)
	yelp_business.registerTempTable("yelp_business")
	val yelp_review_file = "C:/Users/Ashmita/Documents/BigData/Yelp/ExtractedDataset/yelp_academic_dataset_review.json"
	val yelp_review = sqlContext.read.json(yelp_review_file)
	yelp_review.registerTempTable("yelp_review")
	val yelp_user_file = "C:/Users/Ashmita/Documents/BigData/Yelp/ExtractedDataset/yelp_academic_dataset_user.json"
	val yelp_user = sqlContext.read.json(yelp_user_file)
	yelp_user.registerTempTable("yelp_user")
	val review_group = yelp_review.groupBy(yelp_review("business_id")).agg(max(yelp_review("date")))

	val result_busi_rev= yelp_business.join(review_group, yelp_business("business_id") === review_group("business_id")).select(yelp_business("business_id"),yelp_business("name"),yelp_business("stars"),review_group("max(date)"))
	val busi_rev_user_id = yelp_review.join(result_busi_rev, ((yelp_review("business_id") === result_busi_rev("business_id")) && (yelp_review("date") === result_busi_rev("max(date)")))).select(result_busi_rev("business_id"),result_busi_rev("name"),result_busi_rev("stars"),yelp_review("date"),yelp_review("user_id"))

	val result_new = yelp_user.join(busi_rev_user_id,yelp_user("user_id") === busi_rev_user_id("user_id")).select(busi_rev_user_id("business_id"),busi_rev_user_id("name"),busi_rev_user_id("stars"),busi_rev_user_id("date"),busi_rev_user_id("user_id"),yelp_user("average_stars"))

	result_new.write.format("com.databricks.spark.csv").option("header", "true").save("C:/Users/Ashmita/Documents/BigData/Yelp/yelp_business_latest_review_detail.csv")

	sc.stop()
  }
}