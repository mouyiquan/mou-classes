package com.mango.bigdata.spark.core.rdd.operator.acc


import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author mango
 * @description 自定义实现Acc累加器
 * @email
 * @date 2021/4/22
 * @version 1.0
 */
object Spark01_Acc_Custom {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator_Action");
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    val rdd:RDD[String] = sc.makeRDD(List("hello","hello","spark","hadoop"))

    val accumulator = new MyCustomAccumulator
    sc.register(accumulator)

    rdd.foreach( word =>{
      println("11111111")
      accumulator.add(word)
    })

    println(accumulator.value)

    /**
     * 验证复制功能是值传递还是引用传递
     */
    val accumulator2: AccumulatorV2[String, mutable.Map[String, Long]] = accumulator.copy()
    println(accumulator2.value)

    /**
     * 发现值没有变化，需要深思一下为什么
     * 1.是因为行动算子只执行一次？已经没有数据源？  验证发现2个行动算子都执行了
     */
    println("*****再次调用行动算子，验证copy的引用传递还是值传递******")
    rdd.foreach( word =>{
      println("2222222222")
      accumulator.add(word)
    })
    println(accumulator.value)
    println(accumulator2.value)

    //TODO 关闭环境
    sc.stop()
  }

  class MyCustomAccumulator extends AccumulatorV2[String, mutable.Map[String,Long]] {

    private var wcMap = mutable.Map[String,Long]()

    //判断是否初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String,Long]] = {
      val accumulator = new MyCustomAccumulator
      accumulator.wcMap = this.wcMap
      accumulator
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(word: String): Unit = {
      val newCount: Long = wcMap.getOrElse(word, 0L)+1
      wcMap.update(word,newCount)
    }

    //Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String,Long]]): Unit = {
      var map1 = this.value
      var map2 = other.value

      map2.foreach{
        case (word, count) => {
          val newCount: Long = map1.getOrElse(word,0L)+count
          map1.update(word,newCount)
        }
      }
    }

    override def value: mutable.Map[String,Long] = {
      wcMap
    }
  }
}
