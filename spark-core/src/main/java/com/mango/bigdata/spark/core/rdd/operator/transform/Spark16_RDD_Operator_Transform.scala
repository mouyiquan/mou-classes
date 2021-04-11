package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: mango
 * @Description: groupByKey 和 reduceByKey区别
 * @Date: 2021/4/11
 **/

object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)))

    /**
     * groupByKey
     * 1.由于要进行分组等待，所以会保存到文件落盘，否则内存持续等待，可能造成内存溢出
     * 2.spark中，shuffle操作必须落盘处理，不能在内存中数据等待，会导致内存溢出。
     * 3.shuffle的操作性能非常低，因为要进行磁盘IO
     * 4.没有聚合，只进行分组，数据量没有减少，速度比reduceByKey慢
     */
    val value: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    value.collect().foreach(println)

    /**
     * reduceByKey
     * 1.reduceByKey也有shuffle操作
     * 2.支持分区内预先聚合功能：先进行聚合，再进行落盘，减少数据量，磁盘IO更快
     */
    rdd.reduceByKey( (x:Int, y:Int) => {
      println(s"x= ${x},y=${y}")
      x + y
    }).collect().foreach(println)


    //TODO 关闭环境
    sc.stop()
  }
}
