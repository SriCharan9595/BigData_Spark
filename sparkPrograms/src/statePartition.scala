import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

object statePartition {

  def main(args : Array[String]):Unit = {

    println("Job Started...")

    val sparkSession = SparkSession .builder()
      .appName("statePartition")
      .master("local[*]")
      .getOrCreate()

    val authFileRDD = sparkSession.sparkContext.textFile("C:/Users/VC/Downloads/auth.csv", 75)

    val mappedAuthFileRDD = authFileRDD.map(each => {
      val columns = each.split(",")
      (columns(128), columns(0))
    })

    //    pairRDD.foreach(each => println(each))
    //    val distinctStateRDD = pairRDD.distinct().count();
    //    print(distinctStateRDD)

    val csvParitionedRDD = mappedAuthFileRDD.partitionBy(new CustomStatePartitioner)

    csvParitionedRDD.saveAsTextFile("C:/Users/VC/Downloads/BigData/csvStatePartition")

  }

  class CustomStatePartitioner extends Partitioner {

    override def numPartitions: Int = 50

    override def getPartition(key: Any): Int = {

      val input = key.asInstanceOf[String]

      if (input == "Uttar Pradesh")
        return 4
      else if (input == "Tamil Nadu")
        return 18
      else
        return 42

    }

  }

}
