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
object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4),1)


    //1.RDD分区内是有序的，在一个分区里数据是按照逻辑一个个执行的
    //只有前一个数据的所有逻辑执行完毕才会执行下一个数据

    //2.RDD分区外是无序的，不同分区数据计算是无序的

    val value = rdd.map(
      num => {
        println("======"+num)
        num
      }
    )
    val value2 = value.map(
      num => {
        println(">>>>>>>>"+num)
        num
      }
    )
    value2.collect()

    //TODO 关闭环境
      sc.stop()
  }

}
