package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: mango
 * @Description: groupByKey
 * @Date: 2021/4/10
 **/

object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)))

    /**
     * groupByKey 固定是key
     * groupBy  可以指定用key还是value
     */
    val value: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    value.collect().foreach(println)

    //groupBy
    val value2: RDD[(String, Iterable[(String,Int)])] = rdd.groupBy(_._1)
    value2.collect().foreach(println)

    //groupByKey 进阶
    rdd.groupByKey().map(x=>{
      (x._1,x._2.sum)
    }).collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
