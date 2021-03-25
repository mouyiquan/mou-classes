package com.mango.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description RDD文件创建方式
 *             https://www.bilibili.com/video/BV11A411L7CK?p=34spm_id_from=pageDriver第34集
 * @email
 * @date 2021/3/25 10:19
 * @version 1.0
 */
object Spark01_RDD_File1 {

  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从文件中创建RDD，将文件中的数据作为数据源


    //textFile 以行为单位   读取的数据都是字符串
    //wholeTextFiles 以文件为单位
    // 读取的单位为元组，第一个元素表示文件路径，第二个元素表示文件内容
    val rdd = sc.wholeTextFiles("datas")

    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()

  }
}
