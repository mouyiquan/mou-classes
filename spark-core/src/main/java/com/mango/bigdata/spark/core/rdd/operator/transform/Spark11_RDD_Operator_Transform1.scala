package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * sortBy排序shuffle
 */
object Spark11_RDD_Operator_Transform1 {

  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)

    //TODO 算子 - sortBy
    val rdd = sc.makeRDD(List(("1",1),("11",3),("2",2)),2)

    /**
     * sortBy排序shuffle
     * 不会改变分区但是会shuffle
     * 第一个参数指定排序方式
     * 第二个参数为排序方式，默认升序
     */
//    val sortRdd = rdd.sortBy(t=>t._1)
    val sortRdd = rdd.sortBy(t=>t._1.toInt,ascending = false)

    sortRdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
