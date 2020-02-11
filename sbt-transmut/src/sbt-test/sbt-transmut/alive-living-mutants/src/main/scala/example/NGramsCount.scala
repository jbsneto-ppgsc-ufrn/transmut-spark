package example

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.rdd.RDD

object NGramsCount {
  // Start and end marks of a sentence
  val start = "<start>"
  val end = "<end>"
  
  var n: Int = 3

  /*
   * Calculate the n-grams of a given sentence.
   */
  def nGrams(sentence: String): List[List[String]] = {
    val sentenceLowerCase = sentence.trim.toLowerCase
    val tokens = sentenceLowerCase.split(' ').map(t => t.replaceAll("""\W""", "")).filter(_.length() > 0).toList
    val tokensStartEnd = List.fill(n - 1)(start) ++ tokens :+ end
    val ngrams = tokensStartEnd.sliding(n)
    ngrams.toList
  }

  def tokenize(x: String) = x.split("(?<=[a-z])\\.\\s+")

  def filterEmpty(l: List[String]) = l.filter(w => !w.trim.isEmpty).size == n && l != List.fill(n - 1)(start) :+ end

  def countNGrams(input: RDD[String]) = {
    
    val sentences = input.flatMap(tokenize)

    val ngrams = sentences.flatMap((s: String) => nGrams(s))

    val ngramsFiltered = ngrams.filter((ngram: List[String]) => filterEmpty(ngram))

    val ngramsPairs: RDD[(List[String], Int)] = ngramsFiltered.map((ngram: List[String]) => (ngram, 1))

    val ngramsCount = ngramsPairs.reduceByKey((x: Int, y: Int) => x + y)

    ngramsCount
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    var inputURL = "hdfs://master:54310/user/hduser/Yelp/reviews" // default value
    var outputURL = "hdfs://master:54310/user/hduser/Output/ngram-count" // default value
    
    if (args.length > 2 && args(2).toInt > 1) {
      inputURL = args(0)
      outputURL = args(1)
      n = args(2).toInt
    } else {
      println("Invalid arguments")
    }

    val conf = new SparkConf()
    conf.setAppName("NGrams-Count-reduceByKey-non-pre-partitioned-Version-with-" + n.toString)
    val sparkContext = new SparkContext(conf)

    val input = sparkContext.textFile(inputURL)

    val ngramsCount = countNGrams(input)

    ngramsCount.saveAsTextFile(outputURL)
  }
}