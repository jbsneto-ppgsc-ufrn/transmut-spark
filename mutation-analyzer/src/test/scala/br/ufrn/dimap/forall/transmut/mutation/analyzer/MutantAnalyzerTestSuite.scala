package br.ufrn.dimap.forall.transmut.mutation.analyzer

import scala.meta._
import scala.meta.contrib._

import org.scalatest.FunSuite

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.spark.mutation.manager.SparkRDDMetaMutantBuilder
import br.ufrn.dimap.forall.transmut.spark.mutation.manager.SparkRDDMutationManager
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.exception.OriginalTestExecutionException
import br.ufrn.dimap.forall.transmut.mutation.runner.TestFailed
import br.ufrn.dimap.forall.transmut.mutation.runner.TestSuccess
import br.ufrn.dimap.forall.transmut.mutation.runner.TestResult
import br.ufrn.dimap.forall.transmut.mutation.runner.TestError

class MutantAnalyzerTestSuite extends FunSuite {

  test("Test Case 1 - Analyze Mutants With Success and Failed Tests") {

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, String)]) : RDD[(Long, (Double, String))] = {
          val rdd3 = rdd1.join(rdd2)
          rdd3
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.String))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")
    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)
    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(JTR))
    val metaMutantProgramSource = SparkRDDMetaMutantBuilder.buildMetaMutantProgramSourceFromMutantProgramSources(programSource, programSourceMutants)

    val mutantJTR1 = metaMutantProgramSource.mutants(0)
    val mutantJTR2 = metaMutantProgramSource.mutants(1)
    val mutantJTR3 = metaMutantProgramSource.mutants(2)

    val mutantJTR1TestResult = TestFailed(mutantJTR1)
    val mutantJTR2TestResult = TestFailed(mutantJTR2)
    val mutantJTR3TestResult = TestSuccess(mutantJTR3)

    val mutantsResults: List[TestResult[MutantProgramSource]] = List(mutantJTR1TestResult, mutantJTR2TestResult, mutantJTR3TestResult)

    val equivalentsIds: List[Long] = List()

    val mutantsVerdicts = MutantAnalyzer.analyzeMutants(metaMutantProgramSource, mutantsResults, equivalentsIds)

    assert(mutantsVerdicts._1 == metaMutantProgramSource)
    assert(mutantsVerdicts._2.size == 3)
    assert(mutantsVerdicts._2(0) == MutantKilled(mutantJTR1))
    assert(mutantsVerdicts._2(1) == MutantKilled(mutantJTR2))
    assert(mutantsVerdicts._2(2) == MutantLived(mutantJTR3))
  }

  test("Test Case 2 - Analyze Mutants With Equivalent and Error Tests") {

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, String)]) : RDD[(Long, (Double, String))] = {
          val rdd3 = rdd1.join(rdd2)
          rdd3
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.String))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")
    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)
    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(JTR))
    val metaMutantProgramSource = SparkRDDMetaMutantBuilder.buildMetaMutantProgramSourceFromMutantProgramSources(programSource, programSourceMutants)

    val mutantJTR1 = metaMutantProgramSource.mutants(0)
    val mutantJTR2 = metaMutantProgramSource.mutants(1)
    val mutantJTR3 = metaMutantProgramSource.mutants(2)

    val mutantJTR1TestResult = TestSuccess(mutantJTR1)
    val mutantJTR2TestResult = TestSuccess(mutantJTR2)
    val mutantJTR3TestResult = TestError(mutantJTR3)

    val mutantsResults: List[TestResult[MutantProgramSource]] = List(mutantJTR1TestResult, mutantJTR2TestResult, mutantJTR3TestResult)

    val equivalentsIds: List[Long] = List(1, 2)

    val mutantsVerdicts = MutantAnalyzer.analyzeMutants(metaMutantProgramSource, mutantsResults, equivalentsIds)

    assert(mutantsVerdicts._1 == metaMutantProgramSource)
    assert(mutantsVerdicts._2.size == 3)
    assert(mutantsVerdicts._2(0) == MutantEquivalent(mutantJTR1))
    assert(mutantsVerdicts._2(1) == MutantEquivalent(mutantJTR2))
    assert(mutantsVerdicts._2(2) == MutantError(mutantJTR3))
  }

}