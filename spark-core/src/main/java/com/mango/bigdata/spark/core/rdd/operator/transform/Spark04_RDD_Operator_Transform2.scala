package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description TODO
 * @email
 * @date 2021/3/29 14:44
 * @version 1.0
 */
object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    val rdd = sc.makeRDD(List(List(1,2), 3, List(4,5)))


    val value = rdd.flatMap(
      data =>{
        data match {
          case list: List[_] =>  list
          case dar => List(dar)
        }
      }
    )
    
    rdd.collect().foreach(println)
    value.collect().foreach(println)


    //TODO 关闭环境
    sc.stop()
  }

}
