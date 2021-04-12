package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: mango
 * @Description: join 相当于内连接
 * @Date: 2021/4/11
 **/

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd1 = sc.makeRDD(List(("a",1),("b",2),("d",3),("c",4)))
    val rdd2 = sc.makeRDD(List(("a",5),("b",6),("d",7),("c",8)))
    val rdd3 = sc.makeRDD(List(("a",5),("a",6),("d",7),("c",8)))

    var value: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    value.collect().foreach(println)

    rdd1.join(rdd3).collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
