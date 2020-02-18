package example

import java.sql.Date

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/*
 * 3. Join Query
 * 
 * SELECT sourceIP, totalRevenue, avgPageRank
 * 		FROM
 * 			(SELECT sourceIP,
 * 							AVG(pageRank) as avgPageRank,
 * 							SUM(adRevenue) as totalRevenue
 * 			FROM Rankings AS R, UserVisits AS UV
 * 			WHERE R.pageURL = UV.destURL
 * 				AND UV.visitDate BETWEEN Date(`1980-01-01') AND Date(`X')
 * 			GROUP BY UV.sourceIP)
 * 		ORDER BY totalRevenue DESC LIMIT 1
 * 
 * This query joins a smaller table to a larger table then sorts the results.
 */
object JoinQuery {

  /*
   * Rankings
   * 
   * Lists websites and their page rank	
   * 
   * pageURL VARCHAR(300)
   * pageRank INT
   * avgDuration INT
   */
  case class Ranking(pageURL: String, pageRank: Int, avgDuration: Int)

  def parseRankings(line: String): Ranking = {
    val fields = line.split(',')
    val ranking: Ranking = Ranking(fields(0), fields(1).toInt, fields(2).toInt)
    return ranking
  }

  /*
   * UserVisits
   * 
   * Stores server logs for each web page
   * 
   * sourceIP VARCHAR(116)
   * destURL VARCHAR(100)
   * visitDate DATE
   * adRevenue FLOAT
   * userAgent VARCHAR(256)
   * countryCode CHAR(3)
   * languageCode CHAR(6)
   * searchWord VARCHAR(32)
   * duration INT
   */
  case class UserVisit(sourceIP: String, destURL: String, visitDate: Date,
                       adRevenue: Float, userAgent: String, countryCode: String,
                       languageCode: String, searchWord: String, duration: Int)

  def parseUserVisits(line: String): UserVisit = {
    val fields = line.split(',')
    val userVisit: UserVisit = UserVisit(fields(0), fields(1), Date.valueOf(fields(2)),
      fields(3).toFloat, fields(4), fields(5),
      fields(6), fields(7), fields(8).toInt)
    return userVisit
  }
  
  def filterUserVisitsDateRange(u: UserVisit) = {
    val date1 = Date.valueOf("1980-01-01")
    val date2 = Date.valueOf("1980-04-01")
    
    u.visitDate.after(date1) && u.visitDate.before(date2)
  }
  
  def mapUserVisitToTuple(u: UserVisit) = (u.destURL, u)
  
  def mapRankingToTuple(r: Ranking) = (r.pageURL, r)
  
  def mapRankingAndUserVisitJoinToTuple(v: (Ranking, UserVisit)) = (v._2.sourceIP, (v._2.adRevenue, v._1.pageRank))
  
  def mapAggregation(v: (String, Iterable[(Float, Int)])) = (v._1, v._2.map(_._1).sum, v._2.map(_._2).sum / v._2.map(_._2).size)
  
  def join(rankingsLines: RDD[String], userVisitsLines: RDD[String]): RDD[(String, Float, Int)] = {
    val rankings: RDD[Ranking] = rankingsLines.map(parseRankings)
    val userVisits: RDD[UserVisit] = userVisitsLines.map(parseUserVisits)
    val filteredUV: RDD[UserVisit] = userVisits.filter(filterUserVisitsDateRange)
    val subqueryUV: RDD[(String, UserVisit)] = filteredUV.map(mapUserVisitToTuple)
    val subqueryR: RDD[(String, Ranking)] = rankings.map(mapRankingToTuple)
    val subqueryJoin: RDD[(String, (Ranking, UserVisit))] = subqueryR.join(subqueryUV)
    val subqueryJoinValues: RDD[(Ranking, UserVisit)] = subqueryJoin.values
    val subquerySelect: RDD[(String, (Float, Int))] = subqueryJoinValues.map(mapRankingAndUserVisitJoinToTuple)
    val subqueryGroup: RDD[(String, Iterable[(Float, Int)])] = subquerySelect.groupByKey()
    val subqueryAggregation: RDD[(String, Float, Int)] = subqueryGroup.map(mapAggregation)
    val results: RDD[(String, Float, Int)] = subqueryAggregation.sortBy(_._2, false)
    results
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    var inputRankingsURL = "hdfs://master:54310/user/hduser/BigBenchDataSet/rankings" // default value
    var inputUserVisitsURL = "hdfs://master:54310/user/hduser/BigBenchDataSet/uservisits" // default value
    var outputURL = "hdfs://master:54310/user/hduser/Output/join-query-results" // default value

    if (args.length > 2) {
      inputRankingsURL = args(0)
      inputUserVisitsURL = args(1)
      outputURL = args(2)
    } else {
      println("Invalid arguments")
    }

    val conf = new SparkConf()
    conf.setAppName("AMPLab-Big-Data-Benchmark-3-Join-Query-Different-Partitioners")
    val sparkContext = new SparkContext(conf)

    val rankingsLines = sparkContext.textFile(inputRankingsURL)
    val userVisitsLines = sparkContext.textFile(inputUserVisitsURL)
    
    val results = join(rankingsLines, userVisitsLines)

    results.saveAsTextFile(outputURL)
  }
}