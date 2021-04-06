package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * sortBy排序shuffle
 */
object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(6, 2, 4, 1, 5, 3), 2)

    /**
     * sortBy排序shuffle
     */
    val sortRdd: RDD[Int] = rdd.sortBy(num => num);

    sortRdd.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()
  }
}
