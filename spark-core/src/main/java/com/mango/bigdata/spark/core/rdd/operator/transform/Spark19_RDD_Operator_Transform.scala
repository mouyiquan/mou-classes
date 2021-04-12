package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: mango
 * @Description: combineByKey
 * @Date: 2021/4/11
 **/

object Spark19_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),2)

    /**
     * combineByKey : 方法需要三个参数
     * 第一个参数表示：将相同key的第一个数据进行结构化转换，实现操作
     * 第二个参数表示：分区内的计算规则
     * 第三个参数表示：分区间的计算规则
     */
    rdd.combineByKey(
      v => (v,1),
      (t :(Int,Int),v) =>{
        (t._1 + v, t._2+1)
      },
      (t1:(Int,Int),t2 :(Int,Int)) =>{
        (t1._1 + t2._1,t1._2 + t2._2)
      }
    ).collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
