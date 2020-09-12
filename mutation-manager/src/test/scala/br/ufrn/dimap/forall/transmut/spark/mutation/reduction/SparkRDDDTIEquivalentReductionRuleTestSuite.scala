package br.ufrn.dimap.forall.transmut.spark.mutation.reduction

import scala.meta._
import scala.meta.contrib._

import org.scalatest.FunSuite

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.spark.mutation.manager.SparkRDDMutationManager
import br.ufrn.dimap.forall.transmut.spark.mutation.operator.SparkRDDAggregationTransformationReplacement
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum.DTIE

class SparkRDDDTIEquivalentReductionRuleTestSuite extends FunSuite {

  test("Test Case 1 - Non reducible mutants list without DTI") {

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

    assert(!SparkRDDDTIEquivalentReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDDTIEquivalentReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }

  test("Test Case 2 - Non reducible mutants list with DTI") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(FTD, MTR, NFTP, DTI))

    assert(programSourceMutants.size == 6)

    assert(!SparkRDDDTIEquivalentReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDDTIEquivalentReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants == programSourceMutants)
    assert(removedMutants.isEmpty)
  }

  test("Test Case 3 - Reducible mutants with reduceByKey") {

    val idGenerator = LongIdGenerator.generator

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

    assert(SparkRDDDTIEquivalentReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDDTIEquivalentReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 5)
    assert(newMutants.contains(mutantATR1))
    assert(newMutants.contains(mutantATR2))
    assert(newMutants.contains(mutantATR3))
    assert(newMutants.contains(mutantATR4))
    assert(newMutants.contains(mutantATR5))

    assert(removedMutants.size == 1)
    
    val removedMutantDTI = removedMutants(0)
    
    assert(removedMutantDTI.reductionRule == DTIE)
    assert(removedMutantDTI.mutant == mutantDTI)
  }

  test("Test Case 4 - Reducible mutants with combineByKey") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[(Long, (Double, String))]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)
          val rdd3 = rdd2.filter((v: (Long, Double)) => v._2 >= 0)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.String)))))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(ATR, DTI))

    assert(programSourceMutants.size == 7)

    val mutantATR1 = programSourceMutants(0)
    val mutantATR2 = programSourceMutants(1)
    val mutantATR3 = programSourceMutants(2)
    val mutantATR4 = programSourceMutants(3)
    val mutantATR5 = programSourceMutants(4)
    val mutantDTI1 = programSourceMutants(5)
    val mutantDTI2 = programSourceMutants(6)

    assert(SparkRDDDTIEquivalentReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDDTIEquivalentReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 6)
    assert(newMutants.contains(mutantATR1))
    assert(newMutants.contains(mutantATR2))
    assert(newMutants.contains(mutantATR3))
    assert(newMutants.contains(mutantATR4))
    assert(newMutants.contains(mutantATR5))
    assert(newMutants.contains(mutantDTI2))

    assert(removedMutants.size == 1)
    
    val removedMutantDTI1 = removedMutants(0)
    
    assert(removedMutantDTI1.reductionRule == DTIE)
    assert(removedMutantDTI1.mutant == mutantDTI1)
  }

  test("Test Case 5 - Reducible mutants with groupByKey") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[(Long, Double)]) : RDD[(Long, Iterable[Double])] = {
          val rdd2 = rdd1.groupByKey()
          val rdd3 = rdd2.filter((v: (Long, Iterable[Double])) => v._2.size > 0)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), ParameterizedType("Iterable", List(BaseType(BaseTypesEnum.Double))))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), ParameterizedType("Iterable", List(BaseType(BaseTypesEnum.Double))))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(DTI, FTD, NFTP))

    assert(programSourceMutants.size == 4)

    val mutantDTI1 = programSourceMutants(0)
    val mutantDTI2 = programSourceMutants(1)
    val mutantFTD = programSourceMutants(2)
    val mutantNFTP = programSourceMutants(3)

    assert(SparkRDDDTIEquivalentReductionRule.isReducible(programSourceMutants))

    val (newMutants, removedMutants) = SparkRDDDTIEquivalentReductionRule.reduceMutants(programSourceMutants)

    assert(newMutants.size == 3)
    assert(newMutants.contains(mutantDTI2))
    assert(newMutants.contains(mutantFTD))
    assert(newMutants.contains(mutantNFTP))

    assert(removedMutants.size == 1)
    
    val removedMutantDTI1 = removedMutants(0)
    
    assert(removedMutantDTI1.reductionRule == DTIE)
    assert(removedMutantDTI1.mutant == mutantDTI1)
  }
  
}