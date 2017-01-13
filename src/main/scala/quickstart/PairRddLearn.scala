package quickstart

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kyle.xu on 2017/1/10.
  */
object PairRddLearn {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("pair rdd")
    val sparkContext = new SparkContext(sparkConf)
    val readMeLoc = "F:\\Matrix_Study\\spark\\spark-2.0.0-bin-hadoop2.7\\spark-2.0.0-bin-hadoop2.7\\README.md"
    val readMe = sparkContext.textFile(readMeLoc)
    val readMepairs = readMe.map(x => (x.split(" ")(0), x))
    val filterRead = readMepairs.filter { case (k, v) => v.length < 20 }
    //    readMepairs.foreach(println(_))
    //    filterRead.foreach(println(_))

    val words = readMe.flatMap(x => x.split(" "))
    val wordResult = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    wordResult.foreach(println(_))
    println(words.count())

    println("***")
    val pairs = sparkContext.parallelize(List((1, 2), (3, 4), (3, 6)))
    pairs.foreach(println(_))

    println("***")
    val reduceByKey1 = pairs.reduceByKey((x, y) => x + y)
    reduceByKey1.foreach(println(_))

    val groupByKey1 = pairs.groupByKey()
    groupByKey1.foreach(println)

    val flatMapValue1 = pairs.flatMapValues(x => (x to 5))
    flatMapValue1.foreach(println(_))

    println("**")
    val rdd = sparkContext.parallelize(List(("panda", 0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4)))
    val rdd1 = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    rdd1.foreach(println(_))

    println("**")
    val result = rdd.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map {
      case (key, value) => (key, value._1 / value._2.toFloat)
    }
    result.collectAsMap().foreach(println(_))

    println("**")
    val data = Seq(("a", 3), ("b", 4), ("a", 8))
    val dataResult = sparkContext.parallelize(data).reduceByKey((x, y) => x + y,3)
    dataResult.collectAsMap().foreach(println(_))


  }
}
