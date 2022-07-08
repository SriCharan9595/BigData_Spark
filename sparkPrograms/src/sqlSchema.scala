import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object sqlSchema {

  def main(args : Array[String]) = {

    val sparkSession = SparkSession.builder()
      .appName("sqlSchema")
      .master("local[*]")
      .getOrCreate()

    //val sc = sparkSession.sparkContext

    val csvDataDF = sparkSession.read.option("header","true").csv("C:\\Users\\VC\\Downloads\\auth.csv")
    csvDataDF.createOrReplaceTempView("AUTH_TABLE")

    val dataDF = sparkSession.sql("SELECT auth_code, sa from AUTH_TABLE")
    dataDF.show()

    dataDF.printSchema()

    //csvDataDF.select("auth_code","subreq_id","aua","sa").show()

  }
}
