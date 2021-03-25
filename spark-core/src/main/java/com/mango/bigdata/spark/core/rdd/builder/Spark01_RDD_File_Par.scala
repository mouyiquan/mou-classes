package com.mango.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description RDD文件创建方式
 *             https://www.bilibili.com/video/BV11A411L7CK?p=37spm_id_from=pageDriver
 * @email
 * @date 2021/3/25 10:19
 * @version 1.0
 */
object Spark01_RDD_File_Par {

  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //minPartitions 默认最小分区数
    //第二个参数手动设置参数，实际分区数可能比我们设置的大
    //spark读取文件，实际是hadoop读取文件的方式
    //分区数量的计算方式
//    totalSize = 7
//    goalSize = 7 / 3 = 2 （byte）

    val rdd:RDD[String] = sc.textFile("datas/work_count.txt",2)

    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()

  }
}
