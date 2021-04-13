package com.mango.bigdata.spark.core.rdd.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author mango
 * @description 统计各个省份广告点击的top3
 * @email
 * @date 2021/4/12 11:25
 * @version 1.0
 */
object demo1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo1")
    val sc = new SparkContext(sparkConf)

    /**
     * 1.获取原始数据：时间戳、省份、城市、用户、广告
     */
    val dataRDD = sc.textFile("datas/agent.log")

    /**
     * 2.原始数据结构转换
     */
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
      line => {
        val datas = line.split(" ");
        ((datas(1), datas(4)), 1)
      }
    )
    /**
     * 3.数据统计=》分组聚合
     */
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_+_)
    /**
     * 4.数据转换
     */
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }
    /**
     * 5.省份分组，将转换后的数据根据省份进行分组
     */
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

    /**
     * 6.将分组后的数据进行降序排列，并获取前3条数据
     */
    val value: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      (iter: Iterable[(String, Int)]) => {
        iter.toList.sortBy((_: (String, Int))._2)(Ordering.Int.reverse).take(3)
      }
    )
    /**
     * 7.打印数据
     */
    value.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }
}
