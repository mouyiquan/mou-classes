package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_Transform {

  //TODO 准备环境
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
  val sc = new SparkContext(sparkConf)

  //TODO 创建RDD
  /**
   * distinct
   * reduceBykey
   */
  val rdd = sc.makeRDD(List(1,2,3,4,1,2,3))

  val value: RDD[Int] = rdd.distinct()

  value.collect().foreach(println)

  //TODO 关闭环境
  sc.stop()

}
