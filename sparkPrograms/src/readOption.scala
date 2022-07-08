import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object readOption {

  def main(args : Array[String]) = {


    val sparkSession = SparkSession.builder()
      .appName("readOption")
      .master("local[*]")
      .getOrCreate()

    //val sc = sparkSession.sparkContext


    val csvDataDF = sparkSession.read.option("header","true").csv("C:\\Users\\VC\\Downloads\\auth.csv")
    csvDataDF.select("auth_code","subreq_id","aua","sa").show()




  }
}