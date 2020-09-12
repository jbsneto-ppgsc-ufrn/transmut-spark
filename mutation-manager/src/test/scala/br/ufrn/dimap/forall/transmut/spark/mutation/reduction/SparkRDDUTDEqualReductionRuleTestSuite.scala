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

class SparkRDDUTDEqualReductionRuleTestSuite extends FunSuite {

  test("Test Case 1 - Non reducible mutants list with FTD and without UTD, OTD and DTD") {

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

    assert(!SparkRDDUTDEqualReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDUTDEqualReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }
  
  test("Test Case 2 - Non reducible mutants list with OTD and without UTD, FTD and DTD") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(OTD, MTR, NFTP))

    assert(programSourceMutants.size == 3)

    assert(!SparkRDDUTDEqualReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDUTDEqualReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }
  
  test("Test Case 3 - Non reducible mutants list with DTD and without UTD, FTD and OTD") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(DTD, MTR, NFTP))

    assert(programSourceMutants.size == 3)

    assert(!SparkRDDUTDEqualReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDUTDEqualReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }
  
  test("Test Case 4 - Non reducible mutants list with UTD and without DTD, FTD and OTD") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTD, MTR, NFTP))

    assert(programSourceMutants.size == 6)

    assert(!SparkRDDUTDEqualReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDUTDEqualReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }
  
  test("Test Case 5 - Non reducible mutants list without UTD, DTD, FTD and OTD") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR, NFTP, OTI))

    assert(programSourceMutants.size == 3)

    assert(!SparkRDDUTDEqualReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDUTDEqualReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }
  
  test("Test Case 6 - Mutants list with UTD, FTD and other mutants types (without OTD and DTD)") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTD, MTR, NFTP, OTI, FTD))

    assert(programSourceMutants.size == 8)

    val mutantUTD1 = programSourceMutants(0)
    val mutantUTD2 = programSourceMutants(1)
    val mutantUTD3 = programSourceMutants(2)
    val mutantUTD4 = programSourceMutants(3)
    val mutantMTR = programSourceMutants(4)
    val mutantNFTP = programSourceMutants(5)
    val mutantOTI = programSourceMutants(6)
    val mutantFTD = programSourceMutants(7)

    assert(SparkRDDUTDEqualReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDUTDEqualReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 7)
    assert(newMutants.contains(mutantUTD1))
    assert(newMutants.contains(mutantUTD2))
    assert(newMutants.contains(mutantUTD3))
    assert(newMutants.contains(mutantUTD4))
    assert(newMutants.contains(mutantMTR))
    assert(newMutants.contains(mutantNFTP))
    assert(newMutants.contains(mutantOTI))

    assert(removedMutants.size == 1)
    
    val removedMutantFTD = removedMutants(0)
    
    assert(removedMutantFTD.reductionRule == UTDE)
    assert(removedMutantFTD.mutant == mutantFTD)
  }
  
  test("Test Case 7 - Mutants list with UTD, OTD and other mutants types (without FTD and DTD)") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTD, MTR, NFTP, OTI, OTD))

    assert(programSourceMutants.size == 8)

    val mutantUTD1 = programSourceMutants(0)
    val mutantUTD2 = programSourceMutants(1)
    val mutantUTD3 = programSourceMutants(2)
    val mutantUTD4 = programSourceMutants(3)
    val mutantMTR = programSourceMutants(4)
    val mutantNFTP = programSourceMutants(5)
    val mutantOTI = programSourceMutants(6)
    val mutantOTD = programSourceMutants(7)

    assert(SparkRDDUTDEqualReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDUTDEqualReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 7)
    assert(newMutants.contains(mutantUTD1))
    assert(newMutants.contains(mutantUTD2))
    assert(newMutants.contains(mutantUTD3))
    assert(newMutants.contains(mutantUTD4))
    assert(newMutants.contains(mutantMTR))
    assert(newMutants.contains(mutantNFTP))
    assert(newMutants.contains(mutantOTI))

    assert(removedMutants.size == 1)
    
    val removedMutantOTD = removedMutants(0)
    
    assert(removedMutantOTD.reductionRule == UTDE)
    assert(removedMutantOTD.mutant == mutantOTD)
  }
  
  test("Test Case 8 - Mutants list with UTD, DTD and other mutants types (without FTD and OTD)") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTD, MTR, NFTP, OTI, DTD))

    assert(programSourceMutants.size == 8)

    val mutantUTD1 = programSourceMutants(0)
    val mutantUTD2 = programSourceMutants(1)
    val mutantUTD3 = programSourceMutants(2)
    val mutantUTD4 = programSourceMutants(3)
    val mutantMTR = programSourceMutants(4)
    val mutantNFTP = programSourceMutants(5)
    val mutantOTI = programSourceMutants(6)
    val mutantDTD = programSourceMutants(7)

    assert(SparkRDDUTDEqualReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDUTDEqualReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 7)
    assert(newMutants.contains(mutantUTD1))
    assert(newMutants.contains(mutantUTD2))
    assert(newMutants.contains(mutantUTD3))
    assert(newMutants.contains(mutantUTD4))
    assert(newMutants.contains(mutantMTR))
    assert(newMutants.contains(mutantNFTP))
    assert(newMutants.contains(mutantOTI))

    assert(removedMutants.size == 1)
    
    val removedMutantDTD = removedMutants(0)
    
    assert(removedMutantDTD.reductionRule == UTDE)
    assert(removedMutantDTD.mutant == mutantDTD)
  }
  
  test("Test Case 9 - Mutants list with UTD, DTD, FTD and OTD") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTD, MTR, NFTP, OTI, DTD, FTD, OTD))

    assert(programSourceMutants.size == 10)

    val mutantUTD1 = programSourceMutants(0)
    val mutantUTD2 = programSourceMutants(1)
    val mutantUTD3 = programSourceMutants(2)
    val mutantUTD4 = programSourceMutants(3)
    val mutantMTR = programSourceMutants(4)
    val mutantNFTP = programSourceMutants(5)
    val mutantOTI = programSourceMutants(6)
    val mutantDTD = programSourceMutants(7)
    val mutantFTD = programSourceMutants(8)
    val mutantOTD = programSourceMutants(9)

    assert(SparkRDDUTDEqualReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDUTDEqualReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 7)
    assert(newMutants.contains(mutantUTD1))
    assert(newMutants.contains(mutantUTD2))
    assert(newMutants.contains(mutantUTD3))
    assert(newMutants.contains(mutantUTD4))
    assert(newMutants.contains(mutantMTR))
    assert(newMutants.contains(mutantNFTP))
    assert(newMutants.contains(mutantOTI))

    assert(removedMutants.size == 3)
    
    val removedMutantDTD = removedMutants(0)
    val removedMutantFTD = removedMutants(1)
    val removedMutantOTD = removedMutants(2)
    
    assert(removedMutantDTD.reductionRule == UTDE)
    assert(removedMutantDTD.mutant == mutantDTD)
    
    assert(removedMutantFTD.reductionRule == UTDE)
    assert(removedMutantFTD.mutant == mutantFTD)
    
    assert(removedMutantOTD.reductionRule == UTDE)
    assert(removedMutantOTD.mutant == mutantOTD)
  }
  
}