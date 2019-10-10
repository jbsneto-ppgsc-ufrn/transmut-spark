
import org.apache.spark.rdd.RDD

object SparkProgramTestCase1 {
  
  def program(rdd1: RDD[Int]) = {
    val rdd2 = rdd1.map(_.toString())
    val rdd3 = rdd2.persist
    val rdd4 = rdd3.map(v => (v.toInt, v))
    val rdd5 = rdd4.keys
    rdd5
  }
  
}