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

class SparkRDDFTDSubsumptionReductionRuleTestSuite extends FunSuite {

  test("Test Case 1 - Mutants list with FTD, NFTP and other mutants types (without UTD)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          rdd4
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(FTD, MTR, OTD, NFTP, OTI))

    assert(programSourceMutants.size == 5)

    val mutantFTD = programSourceMutants(0)
    val mutantMTR = programSourceMutants(1)
    val mutantOTD = programSourceMutants(2)
    val mutantNFTP = programSourceMutants(3)
    val mutantOTI = programSourceMutants(4)

    assert(SparkRDDFTDSubsumptionReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDFTDSubsumptionReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 4)
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantMTR))
    assert(newMutants.contains(mutantOTD))
    assert(newMutants.contains(mutantOTI))

    assert(removedMutants.size == 1)
    
    val removedMutantNFTP = removedMutants(0)
    
    assert(removedMutantNFTP.reductionRule == FTDS)
    assert(removedMutantNFTP.mutant == mutantNFTP)
  }

  test("Test Case 2 - Mutants list with UTD, NFTP and other mutants types (without FTD)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          rdd4
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTD, MTR, NFTP, OTI))

    assert(programSourceMutants.size == 6)

    val mutantUTD1 = programSourceMutants(0)
    val mutantUTD2 = programSourceMutants(1)
    val mutantUTD3 = programSourceMutants(2)
    val mutantMTR = programSourceMutants(3)
    val mutantNFTP = programSourceMutants(4)
    val mutantOTI = programSourceMutants(5)

    assert(SparkRDDFTDSubsumptionReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDFTDSubsumptionReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 5)
    assert(newMutants.contains(mutantUTD1))
    assert(newMutants.contains(mutantUTD2))
    assert(newMutants.contains(mutantUTD3))
    assert(newMutants.contains(mutantMTR))
    assert(newMutants.contains(mutantOTI))

    assert(removedMutants.size == 1)
    
    val removedMutantNFTP = removedMutants(0)
    
    assert(removedMutantNFTP.reductionRule == FTDS)
    assert(removedMutantNFTP.mutant == mutantNFTP)
  }

  test("Test Case 3 - Mutants list with FTD, UTD, NFTP and other mutants types") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          rdd4
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTD, MTR, NFTP, OTI, FTD))

    assert(programSourceMutants.size == 7)

    val mutantUTD1 = programSourceMutants(0)
    val mutantUTD2 = programSourceMutants(1)
    val mutantUTD3 = programSourceMutants(2)
    val mutantMTR = programSourceMutants(3)
    val mutantNFTP = programSourceMutants(4)
    val mutantOTI = programSourceMutants(5)
    val mutantFTD = programSourceMutants(6)

    assert(SparkRDDFTDSubsumptionReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDFTDSubsumptionReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 6)
    assert(newMutants.contains(mutantUTD1))
    assert(newMutants.contains(mutantUTD2))
    assert(newMutants.contains(mutantUTD3))
    assert(newMutants.contains(mutantMTR))
    assert(newMutants.contains(mutantOTI))
    assert(newMutants.contains(mutantFTD))

    assert(removedMutants.size == 1)
    
    val removedMutantNFTP = removedMutants(0)
    
    assert(removedMutantNFTP.reductionRule == FTDS)
    assert(removedMutantNFTP.mutant == mutantNFTP)
  }

  test("Test Case 4 - Non reducible mutants list with FTD and without NFTP") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          rdd4
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(FTD, MTR, OTD, OTI))

    assert(programSourceMutants.size == 4)

    assert(!SparkRDDFTDSubsumptionReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDFTDSubsumptionReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)

    assert(removedMutants.isEmpty)
  }

  test("Test Case 5 - Non reducible mutants list with UTD and without NFTP") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          rdd4
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTD, MTR, OTI))

    assert(programSourceMutants.size == 5)

    assert(!SparkRDDFTDSubsumptionReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDFTDSubsumptionReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)

    assert(removedMutants.isEmpty)
  }

  test("Test Case 6 - Non reducible mutants list with UTD, FTD and without NFTP") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          rdd4
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTD, MTR, OTI, FTD))

    assert(programSourceMutants.size == 6)

    assert(!SparkRDDFTDSubsumptionReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDFTDSubsumptionReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)

    assert(removedMutants.isEmpty)
  }
  
  test("Test Case 7 - Non reducible mutants list with NFTP and without UTD and FTD") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          rdd4
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(NFTP, MTR, OTI))

    assert(programSourceMutants.size == 3)

    assert(!SparkRDDFTDSubsumptionReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDFTDSubsumptionReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)

    assert(removedMutants.isEmpty)
  }
  
  test("Test Case 8 - Non reducible mutants list without NFTP, UTD and FTD") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          rdd4
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(MTR, OTI, OTD))

    assert(programSourceMutants.size == 3)

    assert(!SparkRDDFTDSubsumptionReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDFTDSubsumptionReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)

    assert(removedMutants.isEmpty)
  }

}