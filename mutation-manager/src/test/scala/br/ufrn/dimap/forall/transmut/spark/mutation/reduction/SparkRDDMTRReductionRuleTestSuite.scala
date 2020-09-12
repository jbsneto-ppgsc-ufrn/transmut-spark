package br.ufrn.dimap.forall.transmut.spark.mutation.reduction

import scala.meta._
import scala.meta.contrib._

import org.scalatest.FunSuite

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.spark.mutation.manager.SparkRDDMutationManager
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum._

class SparkRDDMTRReductionRuleTestSuite extends FunSuite {

  test("Test Case 1 - Reducible MTR mutants - String") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          val rdd5 = rdd4.distinct()
          rdd5
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd5" -> ValReference("rdd5", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(FTD, MTR, NFTP))

    assert(programSourceMutants.size == 3)
    
    val mutantFTD = programSourceMutants(0)
    val mutantMTR = programSourceMutants(1)
    val mutantNFTP = programSourceMutants(2)

    assert(SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 2)
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantNFTP))
    
    assert(removedMutants.size == 1)
    
    val removedMutantMTR = removedMutants(0)
    
    assert(removedMutantMTR.reductionRule == MTRR)
    assert(removedMutantMTR.mutant == mutantMTR)
  }
  
  test("Test Case 2 - Reducible MTR mutants - Int") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.toInt)
          val rdd3 = rdd2.filter((y: Int) => y > 0)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR, FTD, NFTP))

    assert(programSourceMutants.size == 7)

    val mutant0 = programSourceMutants(0)
    val mutant1 = programSourceMutants(1)
    val mutantMax = programSourceMutants(2)
    val mutantMin = programSourceMutants(3)
    val mutantNegative = programSourceMutants(4)
    val mutantFTD = programSourceMutants(5)
    val mutantNFTP = programSourceMutants(6)
    
    assert(SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 5)
    assert(newMutants.contains(mutant0))
    assert(newMutants.contains(mutant1))
    assert(newMutants.contains(mutantNegative))
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantNFTP))
    
    assert(removedMutants.size == 2)
    
    val removedMutantMax = removedMutants(0)
    val removedMutantMin = removedMutants(1)
    
    assert(removedMutantMax.reductionRule == MTRR)
    assert(removedMutantMax.mutant == mutantMax)
    assert(removedMutantMin.reductionRule == MTRR)
    assert(removedMutantMin.mutant == mutantMin)
  }
  
  test("Test Case 3 - Reducible MTR mutants - Float") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.toFloat)
          val rdd3 = rdd2.filter((y: Float) => y > 0.0)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Float)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Float)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR, FTD, NFTP))

    assert(programSourceMutants.size == 7)

    val mutant0 = programSourceMutants(0)
    val mutant1 = programSourceMutants(1)
    val mutantMax = programSourceMutants(2)
    val mutantMin = programSourceMutants(3)
    val mutantNegative = programSourceMutants(4)
    val mutantFTD = programSourceMutants(5)
    val mutantNFTP = programSourceMutants(6)
    
    assert(SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 5)
    assert(newMutants.contains(mutant0))
    assert(newMutants.contains(mutant1))
    assert(newMutants.contains(mutantNegative))
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantNFTP))
    
    assert(removedMutants.size == 2)
    
    val removedMutantMax = removedMutants(0)
    val removedMutantMin = removedMutants(1)
    
    assert(removedMutantMax.reductionRule == MTRR)
    assert(removedMutantMax.mutant == mutantMax)
    assert(removedMutantMin.reductionRule == MTRR)
    assert(removedMutantMin.mutant == mutantMin)
  }
  
  test("Test Case 4 - Reducible MTR mutants - Long") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.toLong)
          val rdd3 = rdd2.filter((y: Long) => y > 0)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Long)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Long)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR, FTD, NFTP))

    assert(programSourceMutants.size == 7)

    val mutant0 = programSourceMutants(0)
    val mutant1 = programSourceMutants(1)
    val mutantMax = programSourceMutants(2)
    val mutantMin = programSourceMutants(3)
    val mutantNegative = programSourceMutants(4)
    val mutantFTD = programSourceMutants(5)
    val mutantNFTP = programSourceMutants(6)
    
    assert(SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 5)
    assert(newMutants.contains(mutant0))
    assert(newMutants.contains(mutant1))
    assert(newMutants.contains(mutantNegative))
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantNFTP))
    
    assert(removedMutants.size == 2)
    
    val removedMutantMax = removedMutants(0)
    val removedMutantMin = removedMutants(1)
    
    assert(removedMutantMax.reductionRule == MTRR)
    assert(removedMutantMax.mutant == mutantMax)
    assert(removedMutantMin.reductionRule == MTRR)
    assert(removedMutantMin.mutant == mutantMin)
  }
  
  test("Test Case 5 - Reducible MTR mutants - Double") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.toDouble)
          val rdd3 = rdd2.filter((y: Double) => y > 0.0)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Double)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Double)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR, FTD, NFTP))

    assert(programSourceMutants.size == 7)

    val mutant0 = programSourceMutants(0)
    val mutant1 = programSourceMutants(1)
    val mutantMax = programSourceMutants(2)
    val mutantMin = programSourceMutants(3)
    val mutantNegative = programSourceMutants(4)
    val mutantFTD = programSourceMutants(5)
    val mutantNFTP = programSourceMutants(6)
    
    assert(SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 5)
    assert(newMutants.contains(mutant0))
    assert(newMutants.contains(mutant1))
    assert(newMutants.contains(mutantNegative))
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantNFTP))
    
    assert(removedMutants.size == 2)
    
    val removedMutantMax = removedMutants(0)
    val removedMutantMin = removedMutants(1)
    
    assert(removedMutantMax.reductionRule == MTRR)
    assert(removedMutantMax.mutant == mutantMax)
    assert(removedMutantMin.reductionRule == MTRR)
    assert(removedMutantMin.mutant == mutantMin)
  }
  
  test("Test Case 6 - Reducible MTR mutants - Char") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.toChar)
          val rdd3 = rdd2.filter((y: Char) => y != ' ')
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Char)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Char)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR, FTD, NFTP))

    assert(programSourceMutants.size == 7)

    val mutant0 = programSourceMutants(0)
    val mutant1 = programSourceMutants(1)
    val mutantMax = programSourceMutants(2)
    val mutantMin = programSourceMutants(3)
    val mutantNegative = programSourceMutants(4)
    val mutantFTD = programSourceMutants(5)
    val mutantNFTP = programSourceMutants(6)
    
    assert(SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 5)
    assert(newMutants.contains(mutant0))
    assert(newMutants.contains(mutant1))
    assert(newMutants.contains(mutantNegative))
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantNFTP))
    
    assert(removedMutants.size == 2)
    
    val removedMutantMax = removedMutants(0)
    val removedMutantMin = removedMutants(1)
    
    assert(removedMutantMax.reductionRule == MTRR)
    assert(removedMutantMax.mutant == mutantMax)
    assert(removedMutantMin.reductionRule == MTRR)
    assert(removedMutantMin.mutant == mutantMin)
  }
  
  test("Test Case 7 - Reducible MTR mutants - List") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.split(" "))
          val rdd3 = rdd2.filter((y: List[String]) => !y.isEmpty)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(ParameterizedType("List", List(BaseType(BaseTypesEnum.String)))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(ParameterizedType("List", List(BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR, FTD, NFTP))

    assert(programSourceMutants.size == 6)

    val mutantHead = programSourceMutants(0)
    val mutantTail = programSourceMutants(1)
    val mutantReverse = programSourceMutants(2)
    val mutantEmpty = programSourceMutants(3)
    val mutantFTD = programSourceMutants(4)
    val mutantNFTP = programSourceMutants(5)
    
    assert(SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)
    
    assert(newMutants.size == 5)
    assert(newMutants.contains(mutantHead))
    assert(newMutants.contains(mutantTail))
    assert(newMutants.contains(mutantEmpty))
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantNFTP))
    
    assert(removedMutants.size == 1)
    
    val removedMutantReverse = removedMutants(0)
    
    assert(removedMutantReverse.reductionRule == MTRR)
    assert(removedMutantReverse.mutant == mutantReverse)
  }
  
  test("Test Case 8 - Reducible MTR mutants - Array") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.split(" ").toArray)
          val rdd3 = rdd2.filter((y: Array[String]) => !y.isEmpty)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(ParameterizedType("Array", List(BaseType(BaseTypesEnum.String)))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(ParameterizedType("Array", List(BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR, FTD, NFTP))

    assert(programSourceMutants.size == 6)

    val mutantHead = programSourceMutants(0)
    val mutantTail = programSourceMutants(1)
    val mutantReverse = programSourceMutants(2)
    val mutantEmpty = programSourceMutants(3)
    val mutantFTD = programSourceMutants(4)
    val mutantNFTP = programSourceMutants(5)
    
    assert(SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)
    
    assert(newMutants.size == 5)
    assert(newMutants.contains(mutantHead))
    assert(newMutants.contains(mutantTail))
    assert(newMutants.contains(mutantEmpty))
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantNFTP))
    
    assert(removedMutants.size == 1)
    
    val removedMutantReverse = removedMutants(0)
    
    assert(removedMutantReverse.reductionRule == MTRR)
    assert(removedMutantReverse.mutant == mutantReverse)
  }
  
  test("Test Case 9 - Reducible MTR mutants - flatMap") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.flatMap((x: String) => x.split(" "))
          val rdd3 = rdd2.filter((y: String) => y != ",")
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ParameterReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR, FTD, NFTP))

    assert(programSourceMutants.size == 6)

    val mutantHead = programSourceMutants(0)
    val mutantTail = programSourceMutants(1)
    val mutantReverse = programSourceMutants(2)
    val mutantEmpty = programSourceMutants(3)
    val mutantFTD = programSourceMutants(4)
    val mutantNFTP = programSourceMutants(5)
    
    assert(SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)
    
    assert(newMutants.size == 5)
    assert(newMutants.contains(mutantHead))
    assert(newMutants.contains(mutantTail))
    assert(newMutants.contains(mutantEmpty))
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantNFTP))
    
    assert(removedMutants.size == 1)
    
    val removedMutantReverse = removedMutants(0)
    
    assert(removedMutantReverse.reductionRule == MTRR)
    assert(removedMutantReverse.mutant == mutantReverse)
  }
  
  test("Test Case 10 - Reducible MTR mutants - General (null)") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        case class Person(name: String)
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => Person(x))
          val rdd3 = rdd2.filter((p: Person) => p.name != "")
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(ClassType("Person")))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(ClassType("Person")))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR, FTD, NFTP))

    assert(programSourceMutants.size == 3)

    val mutantNull = programSourceMutants(0)
    val mutantFTD = programSourceMutants(1)
    val mutantNFTP = programSourceMutants(2)
    
    assert(SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)
    
    assert(newMutants.size == 2)
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantNFTP))
    
    assert(removedMutants.size == 1)
    
    val removedMutantNull = removedMutants(0)
    
    assert(removedMutantNull.reductionRule == MTRR)
    assert(removedMutantNull.mutant == mutantNull)
  }
  
  test("Test Case 11 - Reducible MTR mutants - Tuple (Int, Boolean)") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => (x.toInt, !x.isEmpty))
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Int), BaseType(BaseTypesEnum.Boolean))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR))

    assert(programSourceMutants.size == 8)
    
    val mutant0 = programSourceMutants(0)
    val mutant1 = programSourceMutants(1)
    val mutantMax = programSourceMutants(2)
    val mutantMin = programSourceMutants(3)
    val mutantNegative = programSourceMutants(4)
    val mutantFalse = programSourceMutants(5)
    val mutantTrue = programSourceMutants(6)
    val mutantNegation = programSourceMutants(7)
    
    assert(SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)
    
    assert(newMutants.size == 6)
    assert(newMutants.contains(mutant0))
    assert(newMutants.contains(mutant1))
    assert(newMutants.contains(mutantNegative))
    assert(newMutants.contains(mutantFalse))
    assert(newMutants.contains(mutantTrue))
    assert(newMutants.contains(mutantNegation))
    
    assert(removedMutants.size == 2)
    
    val removedMutantMax = removedMutants(0)
    val removedMutantMin = removedMutants(1)
    
    assert(removedMutantMax.reductionRule == MTRR)
    assert(removedMutantMax.mutant == mutantMax)
    assert(removedMutantMin.reductionRule == MTRR)
    assert(removedMutantMin.mutant == mutantMin)
  }
  
  test("Test Case 12 - Non reducible MTR mutants - Boolean") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => !x.isEmpty)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Boolean)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR))

    assert(programSourceMutants.size == 3)
    
    assert(!SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)
    
    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }
  
  test("Test Case 13 - Non reducible MTR mutants - Set") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => x.split(" ").toSet)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(ParameterizedType("Set", List(BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR))

    assert(programSourceMutants.size == 3)
    
    assert(!SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)
    
    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }
  
  test("Test Case 14 - Non reducible MTR mutants - Option") {
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map((x: String) => if(!x.isEmpty) Some(x) else None)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(ParameterizedType("Option", List(BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR))

    assert(programSourceMutants.size == 1)
    
    assert(!SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)
    
    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }
  
  test("Test Case 15 - Non reducible mutants without MTR") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          val rdd5 = rdd4.distinct()
          rdd5
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd5" -> ValReference("rdd5", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(FTD, NFTP, OTD, OTI, DTI))

    assert(programSourceMutants.size == 7)

    assert(!SparkRDDMTRReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDMTRReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }
  
}