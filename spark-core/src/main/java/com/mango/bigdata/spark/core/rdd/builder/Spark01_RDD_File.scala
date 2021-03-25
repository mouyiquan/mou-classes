package com.mango.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author mango
 * @description RDD文件创建方式
 *             https://www.bilibili.com/video/BV11A411L7CK?p=33spm_id_from=pageDriver第33集
 * @email
 * @date 2021/3/25 10:19
 * @version 1.0
 */
object Spark01_RDD_File {

  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从内存中创建RDD，将内存的数据作为数据源
    //path以当前环境的根路径作为基准，可以写相对路径和绝对路径
    //sc.textFile("C:\Users\jj\IdeaProjects\mou-classes\datas\work_count.txt")
    //path 也可以是目录，会对目录下所有文件操作
//    val rdd:RDD[String] = sc.textFile("datas")
    //path 也可以使用通配符 *
//        val rdd:RDD[String] = sc.textFile("datas/*1.txt")
    //path 也可以是分布式路存储系统路径：HDFS
//    val rdd:RDD[String] = sc.textFile("hdfs://linux1:8088/datas/work_count.txt")

    val rdd:RDD[String] = sc.textFile("datas/work_count.txt")

    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()

  }
}
