package quickstart

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kyle.xu on 2017/1/10.
  */
object RdNormal {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("3.5")
    val sparkContext = new SparkContext(sparkConf)
    val input = sparkContext.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println(result.collect().mkString(","))


    val lines = sparkContext.parallelize(List("hello world", "hello kyle", "hello lala"))
    val words = lines.flatMap(line => line.split(" "))
    println(words.collect().mkString(","))

    val rdd1 = sparkContext.parallelize(List("coffee", "coffee", "panda", "money", "tea"))
    val rdd2 = sparkContext.parallelize(List("coffee", "money", "kitty"))

    val distinctRDD1 = rdd1.distinct()
    println(distinctRDD1.collect().mkString(","))

    val union12 = rdd1.union(rdd2)
    println(union12.collect().mkString(","))
    println(union12.collect())

    val intersect12 = rdd1.intersection(rdd2)
    println(intersect12.collect().mkString(","))

    val subtracr12 = rdd1.subtract(rdd2)
    println(subtracr12.collect().mkString(","))

    val cartesian12 = rdd1.cartesian(rdd2)
    cartesian12.foreach(println(_))

    /*行动操作*/
    val startList = List(5, 2, 3, 4, 5, 5, 4)
    val start = sparkContext.parallelize(startList)

    val collect1 = start.collect().mkString(",")
    println(collect1)

    val fold = start.fold(0)((x, y) => x + y)
    println(fold)

    val aggregator1 = start.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    val avg = aggregator1._1 / aggregator1._2.toDouble
    println(avg)

    println("countByValue")
    val countByValue = start.countByValue()
    println(countByValue)

    val take_ = start.take(2)
    println(take_.mkString(","))

    val top1 = start.top(2)
    println(top1.mkString(","))

    val takeOrdered1 = start.takeOrdered(2)(Ordering.fromLessThan((l, r) => l <= r))
    println(takeOrdered1.mkString(","))

    val takeSample1 = start.takeSample(true, 2)
    println(takeSample1.mkString("-"))

    val reduce1 = start.reduce((x, y) => x + y)
    println(reduce1)

    println(start.sum())


    val persist1 = start.map(x => x * x)
    persist1.persist(StorageLevel.DISK_ONLY)
    println(persist1.count())
    println(persist1.collect().mkString(","))
  }
}
