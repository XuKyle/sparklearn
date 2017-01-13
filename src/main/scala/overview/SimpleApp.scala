package overview

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kyle on 2017/1/9.
  */
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "F:\\Matrix_Study\\spark\\spark-2.0.0-bin-hadoop2.7\\spark-2.0.0-bin-hadoop2.7\\README.md"
    // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2)
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    //    val lineN = logData.count()
    //    val firstLine = logData.first()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    //    println("Line total : %s, first line : %s".format(lineN, firstLine))

    val lineWithPython = logData.filter(line => line.contains("Python"))
    println(lineWithPython.first())
    sc.stop()
  }
}
