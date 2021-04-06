package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 扩大分区demo
 */
object Spark10_RDD_Operator_Transform1 {

  //TODO 准备环境
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
  val sc = new SparkContext(sparkConf)

  val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

  /**
   * coalesce 算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义，不起作用。
   * repartition 实际就是调用coalesce(numPartitions, shuffle = true)
   */
//  val newRdd: RDD[Int] = rdd.coalesce(2);
//  val newRdd: RDD[Int] = rdd.coalesce(3,true);
  val newRdd: RDD[Int] = rdd.repartition(3);

  newRdd.saveAsTextFile("output")

  //TODO 关闭环境
  sc.stop()

}
