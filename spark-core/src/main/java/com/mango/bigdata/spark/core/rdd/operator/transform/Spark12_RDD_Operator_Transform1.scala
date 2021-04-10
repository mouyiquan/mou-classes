package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 双value类型，不同类型的拉链
 */
object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)


//    val rdd1 = sc.makeRDD(List(1,2,3,4,5,6),3)
//    val rdd2 = sc.makeRDD(List(3,4,5,6),2)
    val rdd1 = sc.makeRDD(List(1,2,3,4),2)
    val rdd2 = sc.makeRDD(List(3,4,5,6),2)

    /**
     * 拉链
     * 两个数组的分区数量必须相同，否则抛出错误
     * 两个数组的元素个数也必须相同，否则抛出错误
     */
    val zip = rdd1.zip(rdd2)
    println("rdd1 拉链 rdd2：")
    println(zip.collect().mkString(","))

    //TODO 关闭环境
    sc.stop()
  }
}
