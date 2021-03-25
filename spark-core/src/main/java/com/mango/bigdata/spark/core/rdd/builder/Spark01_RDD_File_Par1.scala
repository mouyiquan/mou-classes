package com.mango.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description 分区很麻烦...
 * @email
 * @date 2021/3/25 10:19
 * @version 1.0
 */
object Spark01_RDD_File_Par1 {

  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //1.数据以行为单位进行读取
//    spark读取文件，采用的是hadoop的方式读取，所以一行一行的读取，和字节数没有关系
    //2.数据读取时以偏移量作为单位,偏移量不会被重复读取
    /**
     * 1@@ => 012
     * 2@@ => 345
     * 3   => 6
     */
    /** 3.数据分区的偏移量范围的计算
     * 0 => [0,3]
     * 1 => [3,6]
     * 2 => [6,7]
     */
    val rdd:RDD[String] = sc.textFile("datas/work_count.txt",2)

    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()

  }
}
