package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 缩小分区demo
 */
object Spark10_RDD_Operator_Transform {

  //TODO 准备环境
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
  val sc = new SparkContext(sparkConf)

  val rdd = sc.makeRDD(List(1,2,3,4,5,6),3)

  /**
   * coalesce方法默认情况下不会将分区的数据打乱重组
   * 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
   * 如果想让数据均衡，可以进行shuffle处理
   * shuffle 第二个参数为true
   */
//  val newRdd: RDD[Int] = rdd.coalesce(2);
  val newRdd: RDD[Int] = rdd.coalesce(2,true);

  newRdd.saveAsTextFile("output")

  //TODO 关闭环境
  sc.stop()

}
