import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object transformAction {

      def main(args: Array[String]): Unit = {
          compute();
      }

      def compute()= {

        val sparkConfig = new SparkConf()
        sparkConfig.setMaster("local[*]")
        sparkConfig.setAppName("TransformAction")

        val sparkContext = new SparkContext(sparkConfig)

        val textFileRDD = sparkContext.textFile("C:/Users/VC/Downloads/auth.csv")

//         textFileRDD.foreach(each => println(each.toUpperCase))

        // textFileRDD.foreach(each => println(each))

//        for (content <- textFileRDD.take(4)) {
//            val cols = content.split(",")
//            println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}|${cols(4)}|${cols(5)}|${cols(6)}|${cols(7)}")
//        }

//         val result = textFileRDD.first()
//        val result = textFileRDD.collect()
//         val result = textFileRDD.count()
         val result = textFileRDD.saveAsTextFile("C:/Users/VC/Documents/AuthOp")
        //val result = textFileRDD.saveAsObjectFile("C:/Users/VC/Documents/")

        println(result)

        // Close the spark context
        sparkContext.stop()

      }

    }