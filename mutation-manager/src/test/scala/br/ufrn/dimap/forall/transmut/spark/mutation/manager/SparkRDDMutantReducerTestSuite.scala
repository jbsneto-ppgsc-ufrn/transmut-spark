package br.ufrn.dimap.forall.transmut.spark.mutation.manager

import scala.meta._
import scala.meta.contrib._

import org.scalatest.FunSuite

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum._

class SparkRDDMutantReducerTestSuite extends FunSuite {

  test("Test Case 1 - Reducible mutants list with UTD, MTR, FTD and NFTP (all reduction rules)") {

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map((a: Int) => a + 1)
          val rdd3 = rdd2.filter((a: Int) => a < 100)
          rdd3
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTD, MTR, FTD, NFTP))

    assert(programSourceMutants.size == 9)

    val mutantUTD1 = programSourceMutants(0)
    val mutantUTD2 = programSourceMutants(1)
    val mutantMTR0 = programSourceMutants(2)
    val mutantMTR1 = programSourceMutants(3)
    val mutantMTRMax = programSourceMutants(4)
    val mutantMTRMin = programSourceMutants(5)
    val mutantMTRNegative = programSourceMutants(6)
    val mutantFTD = programSourceMutants(7)
    val mutantNFTP = programSourceMutants(8)

    val (newMutants, removedMutants) = SparkRDDMutantReducer.reduceMutantsList(programSourceMutants, List(UTDE, FTDS, OTDS, DTIE, ATRC, MTRR))

    assert(newMutants.size == 5)
    assert(newMutants.contains(mutantUTD1))
    assert(newMutants.contains(mutantUTD2))
    assert(newMutants.contains(mutantMTR0))
    assert(newMutants.contains(mutantMTR1))
    assert(newMutants.contains(mutantMTRNegative))

    assert(removedMutants.size == 4)

    val removedMutantFTD = removedMutants(0)
    val removedMutantNFTP = removedMutants(1)
    val removedMutantMTR1 = removedMutants(2)
    val removedMutantMTR2 = removedMutants(3)

    assert(removedMutantFTD.reductionRule == UTDE)
    assert(removedMutantFTD.mutant == mutantFTD)

    assert(removedMutantNFTP.reductionRule == FTDS)
    assert(removedMutantNFTP.mutant == mutantNFTP)

    assert(removedMutantMTR1.reductionRule == MTRR)
    assert(removedMutantMTR1.mutant == mutantMTRMax)

    assert(removedMutantMTR2.reductionRule == MTRR)
    assert(removedMutantMTR2.mutant == mutantMTRMin)
  }

  test("Test Case 2 - Reducible mutants list with FTD, MTR, OTD, NFTP and OTI (all reduction rules)") {

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

    val (newMutants, removedMutants) = SparkRDDMutantReducer.reduceMutantsList(programSourceMutants, List(UTDE, FTDS, OTDS, DTIE, ATRC, MTRR))

    assert(newMutants.size == 2)
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantOTD))

    assert(removedMutants.size == 3)

    val removedMutantNFTP = removedMutants(0)
    val removedMutantOTI = removedMutants(1)
    val removedMutantMTR = removedMutants(2)

    assert(removedMutantNFTP.reductionRule == FTDS)
    assert(removedMutantNFTP.mutant == mutantNFTP)

    assert(removedMutantOTI.reductionRule == OTDS)
    assert(removedMutantOTI.mutant == mutantOTI)

    assert(removedMutantMTR.reductionRule == MTRR)
    assert(removedMutantMTR.mutant == mutantMTR)
  }

  test("Test Case 3 - Reducible mutants list with ATR and DTI (all reduction rules)") {

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.reduceByKey((x: Double, y: Double) => if(x > y) x else y)
          rdd2
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(ATR, DTI))

    assert(programSourceMutants.size == 6)

    val mutantATR1 = programSourceMutants(0)
    val mutantATR2 = programSourceMutants(1)
    val mutantATR3 = programSourceMutants(2)
    val mutantATR4 = programSourceMutants(3)
    val mutantATR5 = programSourceMutants(4)
    val mutantDTI = programSourceMutants(5)

    val (newMutants, removedMutants) = SparkRDDMutantReducer.reduceMutantsList(programSourceMutants, List(UTDE, FTDS, OTDS, DTIE, ATRC, MTRR))

    assert(newMutants.size == 4)
    assert(newMutants.contains(mutantATR1))
    assert(newMutants.contains(mutantATR2))
    assert(newMutants.contains(mutantATR3))
    assert(newMutants.contains(mutantATR4))

    assert(removedMutants.size == 2)

    val removedMutantDTI = removedMutants(0)
    val removedMutantATR = removedMutants(1)

    assert(removedMutantDTI.reductionRule == DTIE)
    assert(removedMutantDTI.mutant == mutantDTI)

    assert(removedMutantATR.reductionRule == ATRC)
    assert(removedMutantATR.mutant == mutantATR5)
  }

  test("Test Case 4 - Non reducible mutants list with Dataflow Mutation Operators (all reduction rules)") {

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map((a: Int) => a + 1)
          val rdd3 = rdd2.filter((a: Int) => a < 100)
          rdd3
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTS, UTR, UTD))

    assert(programSourceMutants.size == 5)

    val (newMutants, removedMutants) = SparkRDDMutantReducer.reduceMutantsList(programSourceMutants, List(UTDE, FTDS, OTDS, DTIE, ATRC, MTRR))

    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }

  test("Test Case 5 - Non reducible mutants list with STR, DTD (all reduction rules)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.union(rdd2)
          val rdd4 = rdd3.distinct
          rdd4
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(STR, DTD))

    assert(programSourceMutants.size == 5)

    val (newMutants, removedMutants) = SparkRDDMutantReducer.reduceMutantsList(programSourceMutants, List(UTDE, FTDS, OTDS, DTIE, ATRC, MTRR))

    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)

  }

}