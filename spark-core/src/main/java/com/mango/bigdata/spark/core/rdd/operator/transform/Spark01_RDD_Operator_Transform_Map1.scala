package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description TODO
 * @email
 * @date 2021/3/25 14:44
 * @version 1.0
 */
object Spark01_RDD_Operator_Transform_Map1 {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    val rdd:RDD[String] = sc.textFile("datas/apache.log")

    val value = rdd.map(
      line => {
        line.split(" ")(6)
      }
    )
    value.collect().foreach(println)

    //TODO 关闭环境
      sc.stop()
  }

}
