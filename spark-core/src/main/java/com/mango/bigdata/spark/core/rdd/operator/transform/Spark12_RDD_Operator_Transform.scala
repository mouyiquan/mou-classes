package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 双value类型，两个集合的交集、并集、差集、拉链
 */
object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)


    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))

    //输出原数组
    println("原数组：")
    println("rdd1",rdd1.collect().mkString(","))
    println("rdd2",rdd2.collect().mkString(","))

    //交集
    val intersection = rdd1.intersection(rdd2)
    println("rdd1 交集 rdd2：")
    println(intersection.collect().mkString(","))

    //并集
    val union = rdd1.union(rdd2)
    println("rdd1 并集 rdd2：")
    println(union.collect().mkString(","))

    //差集
    val subtract = rdd1.subtract(rdd2)
    println("rdd1 差集 rdd2：")
    println(subtract.collect().mkString(","))

    //拉链
    val zip = rdd1.zip(rdd2)
    println("rdd1 拉链 rdd2：")
    println(zip.collect().mkString(","))

    //TODO 关闭环境
    sc.stop()
  }
}
