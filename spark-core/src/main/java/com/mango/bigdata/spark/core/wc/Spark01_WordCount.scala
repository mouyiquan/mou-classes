package com.mango.bigdata.spark.core.wc


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description TODO
 * @email
 * @date 2021/2/2 11:00
 * @version 1.0
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    // TODO 建立与Spark框架的链接
    // JDBC : Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount!")
    val sc = new SparkContext(sparkConf)

    // TODO 执行业务逻辑
    // 1.读取文件，获取一行一行的数据
    val lines:RDD[String] = sc.textFile("datas")

    // 2.拆分成一个一个的单词
    // hello world ->  hello  / world
    val words = lines.flatMap(_.split(" "))

    // 3.将数据根据单词进行分组，便于统计
    // (hello hello) / (world)
    val wordGroup:RDD[(String,Iterable[String])] = words.groupBy(word => word)

    //4.对分组后的数据进行转换
    val wordToCount = wordGroup.map{
      case (str, strings) => {
        (str , strings.size)
      }
    }

    // 5.打印
    val tuples : Array[(String,Int)] = wordToCount.collect()
    tuples.foreach(println)

    // TODO 关闭链接
    sc.stop();
  }
}
