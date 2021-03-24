package com.mango.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description  https://www.bilibili.com/video/BV11A411L7CK?p=33spm_id_from=pageDriver第33集
 * @email
 * @date 2021/3/24 16:01
 * @version 1.0
 */
object Spark01_RDD_Memory {

  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从内存中创建RDD，将内存的数据作为数据源
    val seq = Seq[Int](1,2,3,'啊')

    // parallelize 并行
//    val rdd : RDD[Int] = sc.parallelize(seq)
    val rdd: RDD[Int] = sc.makeRDD(seq)

    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()

  }
}
