package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: mango
 * @Description:  cogroup 和 join不一样
 * @Date: 2021/4/12
 **/

object Spark22_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd1 = sc.makeRDD(List(("a",1),("b",2)))
    val rdd2 = sc.makeRDD(List(("a",5),("b",6),("d",7),("c",8),("c",9)))

    /**
     * cogroup
     * collect  + group
     */
    val value: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    rdd1.cogroup(rdd2)
    value.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
