package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Author: mango
 * @Description: reduceByKey  根据key聚合value
 * @Date: 2021/4/10
 **/

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)))

    /**
     * reduceByKey 将相同key的元组进行value数据聚合操作
     * 打印结果说明:  1.只有一个key的数据没有进行聚合操作
     *              【1,2,3】
     *              【3,3】
     *              【6】
     *             2.也可以自定义值，不一定非要value相加
     */
    rdd.reduceByKey( (x:Int, y:Int) => {
      println(s"x= ${x},y=${y}")
      x + y
    }).collect().foreach(println)


    //TODO 关闭环境
    sc.stop()
  }
}
