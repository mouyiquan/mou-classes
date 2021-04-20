package com.mango.bigdata.spark.core.rdd.operator.partion

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author mango
 * @description 自定义分区器
 * @email
 * @date 2021/4/20
 * @version 1.0
 */
object Spark01_RDD_Operator_Partition {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Partition");
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    //TODO 创建RDD
    val value: RDD[(String, String)] = sc.makeRDD(List(("Hello","111"),("AA","AA"),("BB","BB"),("Hello","222")),3)

    val value1: RDD[(String, String)] = value.partitionBy(new CustomPartition(3))
    value1.saveAsTextFile("output")


    //TODO 关闭环境
    sc.stop()
  }

  class CustomPartition(num: Int) extends Partitioner{

    //分区数量
    override def numPartitions: Int = num

    //返回数据的分区索引，从0开始
    override def getPartition(key: Any): Int = {
      key match {
        case "Hello" => 0
        case "AA" =>1
        case _ => 2
      }
    }
  }

}
