package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * 分区器
 */
object Spark13_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(1,2,3,4,5,6),2)

    /**
     * partitionBy 分区 ，必须是key-value类型
     */
    val mapRDD:RDD[(Int,Int)] = rdd1.map((_,1))

    println(mapRDD.collect().mkString(","))

    /**
     * 分区器 基类 Partitioner
     * 问题思考：
     *  1.如果重分区的分区器和当前RDD分区器一样怎么办
     *    self.partitioner == Some(partitioner)
     *  2.还有其它分区器吗
     *    RangePartitioner、HashPartitioner、PythonPartitioner
     *  3.如果想按照自己的方法进行数据分区怎么办？
     *    实现 Partitioner 分区器
     *
     */
    mapRDD
      .partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output/13")
    //TODO 关闭环境
    sc.stop()
  }
}
