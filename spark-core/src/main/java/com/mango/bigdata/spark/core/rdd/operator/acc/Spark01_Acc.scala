package com.mango.bigdata.spark.core.rdd.operator.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description Acc累加器
 * @email
 * @date 2021/4/21
 * @version 1.0
 */
object Spark01_Acc {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Action");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    val rdd:RDD[Int] = sc.makeRDD(List(3,1,2,4))

    val sum: LongAccumulator = sc.longAccumulator("sum")

    val mapRDD: RDD[Int] = rdd.map(num => {
      sum.add(num)
      num
    })

    println(sum)

    /**
     * 获取累加器的值
     * 少加：转换算子中调用累加器，如果没有行动算子的话，转换算子不会执行，则累加器的值不会变更
     * 多加：转换算子中调用累加器，多次执行行动算子，累加器重复执行
     *  一般情况下，累加器会放置在行动算子中执行，或者使用cache进行缓存。
     */
    mapRDD.collect()
    mapRDD.collect()

    println(sum)


    //TODO 关闭环境
    sc.stop()
  }

}
