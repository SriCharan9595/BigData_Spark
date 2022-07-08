import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object syntax {

  def main(args: Array[String]): Unit = {

    println("Hello world!")

    // 1. Spark Config (we have to mention created spark config in import stmt in line 1)
    val sparkConfig = new SparkConf() // creating a spark config
    sparkConfig.setMaster("local[*]") // setting up env with created config, either local or cluster
    sparkConfig.setAppName("computeSpark") // naming the project with created config

    // 2. Spark Context (we have to mention created spark context in import stmt in line 1)
    val sparkContext = new SparkContext(sparkConfig) // creating a spark context with spark config

    // 3. Adding Transformations
    val txtFileRDD = sparkContext.textFile("C:/Users/VC/Downloads/auth.csv") // adding transformation with spark context

    // 4. Adding Action
    //txtFileRDD.foreach(content => println(content)) // adding action with the transformation value
    for (content <- txtFileRDD.take(4)) {
      val cols = content.split(",")
      println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}|${cols(4)}|${cols(5)}|${cols(6)}|${cols(7)}")
    }

    // 5. Close the Spark Context
    sparkContext.stop()

  }

}