package com.mango.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: mango
 * @Description: combineByKey、reduceByKey、foldByKey、aggregateByKey
 * @Date: 2021/4/11
 **/

object Spark20_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),2)


    rdd.reduceByKey(_+_).collect().foreach(println)
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
    rdd.foldByKey(0)(_+_).collect().foreach(println)
    rdd.combineByKey( v=>v ,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y).collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
