package example

import org.apache.spark._
import org.apache.spark.rdd.RDD

object WordCount {

	def wordCount(input: RDD[String]) = {
		val words = input.flatMap( (line: String) => line.split(" ") )
		val pairs = words.map( (word: String) => (word, 1) )
		val counts = pairs.reduceByKey( (a: Int, b: Int) => a + b )
		counts
	}
	
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
    	conf.setAppName("Word-Count")
    	val sc = new SparkContext(conf)
		val textFile = sc.textFile("hdfs://...")
		val results = wordCount(textFile)
		results.saveAsTextFile("hdfs://...")
	}
  
}