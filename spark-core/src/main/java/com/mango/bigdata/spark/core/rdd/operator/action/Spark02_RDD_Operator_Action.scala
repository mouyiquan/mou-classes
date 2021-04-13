package com.mango.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description 行动算子aggregate
 * @email
 * @date 2021/4/13
 * @version 1.0
 */
object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Action");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    val rdd:RDD[Int] = sc.makeRDD(List(3,1,2,4),2)

    /**
     * 预期13+17 = 30 结果40
     * aggregateByKey: 初始值只会参与分区内计算
     * aggregate： 初始值会参与分区内和分区间计算
     */
    val i: Int = rdd.aggregate(10)(_+_,_+_)
    println(i)

    //TODO 关闭环境
    sc.stop()
  }

}
