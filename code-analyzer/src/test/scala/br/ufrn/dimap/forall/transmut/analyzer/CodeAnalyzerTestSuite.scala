package br.ufrn.dimap.forall.transmut.analyzer

import scala.meta._
import scala.meta.Tree
import scala.meta.contrib._

import org.scalatest.FunSuite

class CodeAnalyzerTestSuite extends FunSuite {
  
  test("Test Case 1 - BaseTypesTestCase.scala"){
    
    val tree = CodeAnalyzer.getTreeFromPath("./src/test/resources/BaseTypesTestCase.scala")
    
    val sourceContentTree = tree match {
      case Source(content) => Some(content.head)
      case _ => None
    }
    
    val expectedContentTree: Tree = q"""
      object TypesAnalyzerBaseTypesTestCase {
        val intValue = 0
        val stringValue = ""
        val floatValue = 0.0f
        val doubleValue = 0.0d
        val charValue = '0'
        val longValue = 0l
        val booleanValue = false
      }"""
    
    
    assert(sourceContentTree.isDefined)
    assert(sourceContentTree.get.structure == expectedContentTree.structure)
    
  }
  
  test("Test Case 2 - SparkProgramTestCase1.scala"){
    
    val tree = CodeAnalyzer.getTreeFromPath("./src/test/resources/SparkProgramTestCase1.scala")
    
    val sourceContentTree = tree match {
      case Source(content) => Some(content)
      case _ => None
    }
    
    val expectedTree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgramTestCase1 {
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map(_.toString())
          val rdd3 = rdd2.persist
          val rdd4 = rdd3.map(v => (v.toInt, v))
          val rdd5 = rdd4.keys
          rdd5
        }
      }"""
    
    val expectedContentTree = expectedTree match {
      case Term.Block(content) => Some(content)
      case _ => None
    }
    
    
    assert(sourceContentTree.isDefined)
    assert(expectedContentTree.isDefined)
    assert(sourceContentTree.get.structure == expectedContentTree.get.structure)
    
  }

}