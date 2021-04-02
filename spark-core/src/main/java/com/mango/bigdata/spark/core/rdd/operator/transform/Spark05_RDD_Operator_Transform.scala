package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description
 * @email
 * @date
 * @version 1.0
 */
object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    /**
     * glom() 函数说明
     * 将分区数据的原类型转化为数组原类型进行返回，分区不变
     */

    //glom 将分区数据作为数组结果返回
    val glomRDD:RDD[Array[Int]] = rdd.glom()

    val value = glomRDD.map(array => {
      array.max
    })

    println(value.collect().sum)

    //TODO 关闭环境
    sc.stop()
  }

}
