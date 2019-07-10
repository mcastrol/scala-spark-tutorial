package com.sparkTutorial.pairRdd.sort

import com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice.AvgCount
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("SortedWordCountProblem").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")

    val words = lines.flatMap(line => line.split(" "))

    val wordPairRdd = words.map(word => (word, 1))

    val wordToCountPairs = wordPairRdd.reduceByKey((x, y) => x + y)

    val countToWordParis = wordToCountPairs.map(wordToCount => (wordToCount._2, wordToCount._1))


    val sortedCountToWordParis = countToWordParis.sortByKey(ascending = false)

    println("before map")
    for ((word, count) <-  sortedCountToWordParis.collect()) println(word + " : " + count)
    val sortedWordToCountPairs = sortedCountToWordParis.map(countToWord => (countToWord._2, countToWord._1))

    println("final")
    for ((word, count) <- sortedWordToCountPairs.collect()) println(word + " : " + count)

    println("solved with sort by")
    val sortByWordCounts = wordToCountPairs.sortBy(wordCount => wordCount._2, ascending = false)
    for ((word, count) <- sortByWordCounts.collect()) println(word + " : " + count)


  }

}

