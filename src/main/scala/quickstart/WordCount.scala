package quickstart

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kyle.xu on 2017/1/10.
  */
object WordCount {
  def main(args: Array[String]) {
    val inputFile = "F:\\Matrix_Study\\spark\\spark-2.0.0-bin-hadoop2.7\\spark-2.0.0-bin-hadoop2.7\\README.md"
    val outputFile = "F:\\sparklearn\\WordCount"

    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)
    val file = sc.textFile(inputFile)
    val words = file.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey {
      case (x, y) => x + y
    }
    counts.saveAsTextFile(outputFile)

    sc.stop()
  }
}
