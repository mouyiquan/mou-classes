package com.mango.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mango
 * @description TODO
 * @email
 * @date 2021/4/13
 * @version 1.0
 */
object Spark04_WordCount {
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount!")
    val sc = new SparkContext(sparkConf)

    wordCount1(sc)
    wordCount2(sc)
    wordCount3(sc)
    wordCount4(sc)
    wordCount5(sc)

    sc.stop();
  }

  /**
   * groupBy
   * @param sc
   */
  def wordCount1(sc:SparkContext): Unit ={
    val data: RDD[String] = sc.makeRDD(List("Hello Spark","Hello Scala"))

    val flatData: RDD[String] = data.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = flatData.groupBy(word=>word)
    val mapValuesData: RDD[(String, Int)] = wordGroup.mapValues(iter=>iter.size)

    mapValuesData.collect().foreach(println)
  }

  /**
   * groupByKey 有shuffle，效率低，而且数据量不会减少,可以使用reduceByKey优化数据量
   * @param sc
   */
  def wordCount2(sc:SparkContext): Unit ={
    val data: RDD[String] = sc.makeRDD(List("Hello Spark","Hello Scala"))
    val flatData: RDD[String] = data.flatMap(_.split(" "))

    val mapOne: RDD[(String, Int)] = flatData.map((_,1))
    val value: RDD[(String, Iterable[Int])] = mapOne.groupByKey()

    value.mapValues(iter=>iter.size).collect().foreach(println)
  }

  /**
   * reduceByKey
   * @param sc
   */
  def wordCount3(sc:SparkContext): Unit ={
    val data: RDD[String] = sc.makeRDD(List("Hello Spark","Hello Scala"))
    val flatData: RDD[String] = data.flatMap(_.split(" "))

    val mapOne: RDD[(String, Int)] = flatData.map((_,1))
    val value: RDD[(String, Int)] = mapOne.reduceByKey(_+_)

    value.collect().foreach(println)
  }

  /**
   * aggregateByKey
   * @param sc
   */
  def wordCount4(sc:SparkContext): Unit ={
    val data: RDD[String] = sc.makeRDD(List("Hello Spark","Hello Scala"))
    val flatData: RDD[String] = data.flatMap(_.split(" "))

    val mapOne: RDD[(String, Int)] = flatData.map((_,1))
    val value: RDD[(String, Int)] = mapOne.aggregateByKey(0)(_+_,_+_)

    value.collect().foreach(println)
  }

  /**
   * foldByKey
   * @param sc
   */
  def wordCount5(sc:SparkContext): Unit ={
    val data: RDD[String] = sc.makeRDD(List("Hello Spark","Hello Scala"))
    val flatData: RDD[String] = data.flatMap(_.split(" "))

    val mapOne: RDD[(String, Int)] = flatData.map((_,1))
    val value: RDD[(String, Int)] = mapOne.foldByKey(0)(_+_)

    value.collect().foreach(println)
  }

  /**
   * foldByKey
   * @param sc
   */
  def wordCount6(sc:SparkContext): Unit ={
    val data: RDD[String] = sc.makeRDD(List("Hello Spark","Hello Scala"))
    val flatData: RDD[String] = data.flatMap(_.split(" "))

    val mapOne: RDD[(String, Int)] = flatData.map((_,1))
    val value: RDD[(String, Int)] = mapOne.combineByKey(
      v=>v,
      (x:Int,y) => x+y,
      (x:Int,y:Int) => x+y
    )

    value.collect().foreach(println)
  }


  /**
   * countByKey
   * @param sc
   */
  def wordCount7(sc:SparkContext): Unit ={
    val data: RDD[String] = sc.makeRDD(List("Hello Spark","Hello Scala"))
    val flatData: RDD[String] = data.flatMap(_.split(" "))

    val mapOne: RDD[(String, Int)] = flatData.map((_,1))
    val stringToLong: collection.Map[String, Long] = mapOne.countByKey()

    stringToLong.foreach(println)
  }

  /**
   * countByKey
   * @param sc
   */
  def wordCount8(sc:SparkContext): Unit ={
    val data: RDD[String] = sc.makeRDD(List("Hello Spark","Hello Scala"))
    val flatData: RDD[String] = data.flatMap(_.split(" "))

    val stringToLong: collection.Map[String, Long] = flatData.countByValue()
    stringToLong.foreach(println)
  }
}
