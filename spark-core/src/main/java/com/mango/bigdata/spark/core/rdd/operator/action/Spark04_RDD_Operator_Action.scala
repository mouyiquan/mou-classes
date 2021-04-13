package com.mango.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description 行动算子fold
 * @email
 * @date 2021/4/13
 * @version 1.0
 */
object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Action");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    val rdd:RDD[Int] = sc.makeRDD(List(3,1,3,4),2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",1),("a",2),("b",1),("b",3),("c",2)),2)

    val stringToLong: collection.Map[String, Long] = rdd2.countByKey()
    println(stringToLong)

    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    println(intToLong)

    //TODO 关闭环境
    sc.stop()
  }

}
