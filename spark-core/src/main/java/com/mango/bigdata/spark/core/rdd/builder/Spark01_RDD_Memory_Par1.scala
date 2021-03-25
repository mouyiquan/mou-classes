package com.mango.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description  分区并行执行
 *             https://www.bilibili.com/video/BV11A411L7CK?p=36spm_id_from=pageDriver
 * @email
 * @date 2021/3/24 16:01
 * @version 1.0
 */
object Spark01_RDD_Memory_Par1 {

  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //[1,4]，[2,5],[3] 错
    //[1]，[2,3],[4,5] 真
    // 分区源码parallelize
    //核心下标算法
//    positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
//      (0 until numSlices).iterator.map { i =>
//        val start = ((i * length) / numSlices).toInt
//        val end = (((i + 1) * length) / numSlices).toInt
//        (start, end)
//      }

    val rdd = sc.makeRDD(List(1,2,3,4,5),3)

    //将处理的分区数据保存为文件
    rdd.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()

  }
}
