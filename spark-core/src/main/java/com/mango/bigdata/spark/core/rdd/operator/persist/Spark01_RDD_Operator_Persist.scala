package com.mango.bigdata.spark.core.rdd.operator.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description 持久化
 * @email
 * @date 2021/4/20
 * @version 1.0
 */
object Spark01_RDD_Operator_Persist {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Action");
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    //TODO 创建RDD
    val rdd:RDD[Int] = sc.makeRDD(List(3,1,2,4),2)

    val mapRDD: RDD[Int] = rdd.map(_ *2)

    /**
     * cache 存储在内存中，可能造成内存溢出，效率高
     *
     * persist 存储在磁盘中，IO，效率低，数据安全，job执行完成临时文件夹被删除
     *
     * checkpoint 存储在磁盘中，IO，效率低，数据安全，job执行完成不会删除文件
     */

    //持久化数据
    mapRDD.cache()
    mapRDD.persist()
    mapRDD.checkpoint()

    val i: Int = mapRDD.aggregate(2)(_+_,_+_)
    println(i)

    val i1: Int = mapRDD.reduce(_+_)
    println(i1)




    //TODO 关闭环境
    sc.stop()
  }

}
