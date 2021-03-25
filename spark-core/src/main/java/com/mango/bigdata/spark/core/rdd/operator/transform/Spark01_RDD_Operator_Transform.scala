package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author mango
 * @description TODO
 * @email
 * @date 2021/3/25 14:44
 * @version 1.0
 */
object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    val rdd:RDD[Int] = sc.makeRDD(List(3,1,2,4))

    def mapFunction(num:Int):Int = {
      num * 2
    }

//    val value = rdd.map(mapFunction)
//    val value = rdd.map((num:Int)=>{ num * 2})
//    val value = rdd.map((num:Int)=> num * 2)
//    val value = rdd.map(num => num * 2)
    val value = rdd.map(_ * 2)


    value.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }

}
