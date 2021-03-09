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
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount!")
    val sc = new SparkContext(sparkConf)

    val lines:RDD[String] = sc.textFile("datas")

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => (word,1)
    )

    val wordGroup:RDD[(String,Iterable[(String,Int)])] = wordToOne.groupBy(
      t => t._1
    )

    val wordToCount = wordGroup.map{
      case (str, strings) => {
        strings.reduce(
          (t1, t2) => {
            (t1._1,t1._2 + t2._2)
          }
        )
      }
    }

    val tuples : Array[(String,Int)] = wordToCount.collect()
    tuples.foreach(println)

    sc.stop();
  }
}
