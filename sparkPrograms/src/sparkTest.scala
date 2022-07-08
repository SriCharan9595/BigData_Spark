import org.apache.spark.sql.SparkSession

object sparkTest {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .appName("CodaJOB")
      .master("local[*]")
      .getOrCreate()

    val sc = ss.sparkContext

    val listRDD = sc.parallelize(List("Couchdb", "hbase", "cassandra"))

    listRDD.collect().foreach(each => println(each))

  }
}
