package com.mango.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description  分区并行执行
 *             https://www.bilibili.com/video/BV11A411L7CK?p=35spm_id_from=pageDriver
 * @email
 * @date 2021/3/24 16:01
 * @version 1.0
 */
object Spark01_RDD_Memory_Par {

  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");

    sparkConf.set("spark.default.parallelism","5");

    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //makeRDD第二个参数表示分区数
    //第二个参数可以不传，有默认值defaultParallelism(1.默认并行数)
    // 2.setMaster("local[*]") ，默认取这个值，如果没有就走下一步
    // 3.默认值为CPU的核数（例如八核16线程）
//    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val rdd = sc.makeRDD(List(1,2,3,4))

    //将处理的分区数据保存为文件
    rdd.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()

  }
}
