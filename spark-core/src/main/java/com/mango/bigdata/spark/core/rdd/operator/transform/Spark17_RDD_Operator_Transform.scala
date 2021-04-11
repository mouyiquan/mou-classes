package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: mango
 * @Description: aggregateByKey
 * @Date: 2021/4/11
 **/

object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),2)

    /**
     * reduceByKey 分区内和分区间计算方式  相同
     * aggregateByKey 分区内和分区间计算方式  不同
     * 第一个参数：
     *    zeroValue:表示初始值，在分区内计算的时候，第一个数和初始值比较才有参照物
     * 第二个参数（参数为两个函数）：
     *    第一个函数：分区内计算方式
     *    第二个函数：分区间计算方式
     */
    rdd.aggregateByKey(0)(
      (x,y) => math.max(x,y),
      (x,y) => x+y
    ).collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
