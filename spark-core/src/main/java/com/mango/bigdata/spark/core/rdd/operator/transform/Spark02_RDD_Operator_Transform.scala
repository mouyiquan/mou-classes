package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description TODO
 * @email
 * @date 2021/3/29 14:44
 * @version 1.0
 */
object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    /**
     * mapPartitions： 可以以分区作为单位进行数据转换操作
     *                  但是会将整个分区的数据加载到内存进行引用
     *                  处理完的数据是不会被释放的，存在对象的引用。
     *                  在内存较小数据较大的情况下，会造成内存溢出。
     */
    val value = rdd.mapPartitions(
      iter => {
        println(">>>>>>>>>>>>>")
        iter.map(_ * 2)
      }
    )
    value.collect()

    //TODO 关闭环境
    sc.stop()
  }

}
