package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * 分区
 */
object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(1,2,3,4,5,6),2)

    /**
     * partitionBy 分区 ，必须是key-value类型
     */
    val mapRDD:RDD[(Int,Int)] = rdd1.map((_,1))

    println(mapRDD.collect().mkString(","))

    /**
     * RDD => PairRDDFunctions
     * 隐式转换 implicit def rddToPairRDDFunctions   implicit(隐式函数)
     */
    mapRDD
      .partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output/13")
    //TODO 关闭环境
    sc.stop()
  }
}
