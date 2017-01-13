package quickstart

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kyle.xu on 2017/1/10.
  */
object RDDRelation {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD create")
    val sparkContext = new SparkContext(sparkConf)
    val lines = sparkContext.parallelize(List("kyle", "xu"))
    println("lines.count(): " + lines.count())

    val readme = sparkContext.textFile("F:\\Matrix_Study\\spark\\spark-2.0.0-bin-hadoop2.7\\spark-2.0.0-bin-hadoop2.7\\README.md")
    println(readme)
    println(readme.count())

    val logFile = sparkContext.textFile("F:\\sparklearn\\aqplus.2017-01-03.log")
    val loginFile = logFile.filter(line => line.contains("登录"))
    //    loginFile.foreach(line => println(line))

    println("总计登录了多少次：" + loginFile.count())
    println("登录前10位的人:")
    loginFile.take(10).foreach(line => println(line.split("customer:\\[")(1).split("\\]")(0)))


    sparkContext.stop()
  }
}

/*
class SearchFunctions(val query: String) {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchesFunctionReference(rdd: RDD[String]): RDD[String] = {
    rdd.map(isMatch)
  }

  def getMathesFieldReference(rdd: RDD[String]): RDD[String] = {
    rdd.map(line => line)
  }

  def getMatchesNoReference(rdd: RDD[String]): RDD[String] = {
    val query_ = this.query
    rdd.map(x => x.split(query_))
  }
}*/
