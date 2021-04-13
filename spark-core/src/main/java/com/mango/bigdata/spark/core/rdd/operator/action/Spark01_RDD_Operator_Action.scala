package com.mango.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description 行动算子
 * @email
 * @date 2021/4/13
 * @version 1.0
 */
object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Action");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    val rdd:RDD[Int] = sc.makeRDD(List(3,1,2,4))


    /**
     * collect
     * 1.触发作业（JOB）的执行方法
     * 2.底层调用 sc.runJob -> dagScheduler -> submitJob -> eventProcessLoop.post(JobSubmitted) -> ActiveJob
     * 3.collect方法会将不同分区的数据按照分区顺序采集到Driver端的内存中，形成数组
     */
    rdd.collect().foreach(println)

    /**
     * reduce
     */
    println(rdd.reduce((_: Int) + (_: Int)))


    /**
     * count
     * 计数
     */
    println(rdd.count())

    /**
     * first
     * 获取第一个值
     */
    println(rdd.first())

    /**
     * take
     * 获取n个数据
     */
    println(rdd.take(2).mkString(","))

    /**
     * takeOrdered
     * 获取n个排过序的数据
     * 默认升序，可以自定义排序方式
     */
    println(rdd.takeOrdered(3)(Ordering.Int.reverse).mkString(","))

    //TODO 关闭环境
    sc.stop()
  }

}
