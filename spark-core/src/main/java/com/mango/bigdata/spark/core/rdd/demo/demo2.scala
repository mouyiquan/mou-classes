package com.mango.bigdata.spark.core.rdd.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description TODO
 * @email
 * @date 2021/5/7 14:52
 * @version 1.0
 */
object demo2 {

  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo2")
    val sc = new SparkContext(sparkConf)



    //TODO 关闭环境
    sc.stop()
  }

}
