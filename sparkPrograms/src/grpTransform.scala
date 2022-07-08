import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object grpTransform {

  def main(args : Array[String]) = {

  // 1. Spark Config
  val sparkConfig = new SparkConf()
  sparkConfig.setMaster("local[*]")
  sparkConfig.setAppName("grpTransform")

  // 2. Spark Context
  val sparkContext = new SparkContext(sparkConfig)
  //sparkContext.setCheckpointDir("hdfs://localhost:9000/ac/cp1")

  sparkContext.textFile("C:\\Users\\VC\\Downloads\\motivation.txt")
    .flatMap(each => each.split(" "))
    .map(eachWord => (eachWord,1))
    .reduceByKey(_+_)
    .foreach(println)

  // 3. Transformation
  val textFileRDD = sparkContext.textFile("C:\\Users\\VC\\Downloads\\motivation.txt")

  val splittedDataRDD = textFileRDD.flatMap(each => each.split(" "))

  val mappedRDD = splittedDataRDD.map(eachWord => (eachWord,1))

  val reductionRDD = mappedRDD.reduceByKey((x,y) => x+y)

  reductionRDD.foreach(println)

  sparkContext.stop()

  }

}