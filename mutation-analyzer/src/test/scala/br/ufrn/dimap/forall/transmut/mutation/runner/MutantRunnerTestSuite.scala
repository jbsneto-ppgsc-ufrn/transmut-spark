package br.ufrn.dimap.forall.transmut.mutation.runner

import scala.meta._
import scala.meta.contrib._

import org.scalatest.FunSuite
import org.scalamock.scalatest.MockFactory

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.spark.mutation.manager.SparkRDDMetaMutantBuilder
import br.ufrn.dimap.forall.transmut.spark.mutation.manager.SparkRDDMutationManager
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.exception.OriginalTestExecutionException

class MutantRunnerTestSuite extends FunSuite with MockFactory {

  test("Tast Case 1 - Original Program Test Success") {

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

    val equivalentMutants: List[Long] = List(2)

    val runOriginalTestMock = mockFunction[ProgramSource, TestResult[ProgramSource]]
    val runMutantTestMock = mockFunction[MutantProgramSource, List[Long], Boolean, List[Long], TestResult[MutantProgramSource]]

    inSequence {
      runOriginalTestMock.expects(metaMutantProgramSource.original).returning(TestSuccess(metaMutantProgramSource.original)).once
      runMutantTestMock.expects(mutantJTR1, equivalentMutants, false, List()).returning(TestFailed(mutantJTR1)).once
      runMutantTestMock.expects(mutantJTR2, equivalentMutants, false, List()).returning(TestError(mutantJTR2)).once
      runMutantTestMock.expects(mutantJTR3, equivalentMutants, false, List()).returning(TestSuccess(mutantJTR3)).once
    }

    val mutantRunner = new MutantRunner {
      def runOriginalTest(programSource: ProgramSource): TestResult[ProgramSource] = runOriginalTestMock(programSource)
      def runMutantTest(mutant: MutantProgramSource, equivalentMutants: List[Long], testOnlyLivingMutants: Boolean = false, livingMutants: List[Long] = List(), forceExecutionEnabled: Boolean = false, forceExecution: List[Long] = List()): TestResult[MutantProgramSource] = runMutantTestMock(mutant, equivalentMutants, testOnlyLivingMutants, livingMutants)
    }

    val testsVerdicts = mutantRunner.runMutationTestProcess(metaMutantProgramSource, equivalentMutants, false, List())

    assert(testsVerdicts._1 == metaMutantProgramSource)
    assert(testsVerdicts._2.size == 3)
    assert(testsVerdicts._2(0) == TestFailed(mutantJTR1))
    assert(testsVerdicts._2(1) == TestError(mutantJTR2))
    assert(testsVerdicts._2(2) == TestSuccess(mutantJTR3))
  }

  test("Tast Case 2 - Original Program Test Failed") {

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

    val equivalentMutants: List[Long] = List(2)

    val runOriginalTestMock = mockFunction[ProgramSource, TestResult[ProgramSource]]
    val runMutantTestMock = mockFunction[MutantProgramSource, List[Long], Boolean, List[Long], TestResult[MutantProgramSource]]

    runOriginalTestMock.expects(metaMutantProgramSource.original).returning(TestFailed(metaMutantProgramSource.original)).once
    runMutantTestMock.expects(*, *, *, *).never()

    val mutantRunner = new MutantRunner {
      def runOriginalTest(programSource: ProgramSource): TestResult[ProgramSource] = runOriginalTestMock(programSource)
      def runMutantTest(mutant: MutantProgramSource, equivalentMutants: List[Long], testOnlyLivingMutants: Boolean = false, livingMutants: List[Long] = List(), forceExecutionEnabled: Boolean = false, forceExecution: List[Long] = List()): TestResult[MutantProgramSource] = runMutantTestMock(mutant, equivalentMutants, testOnlyLivingMutants, livingMutants)
    }

    assertThrows[OriginalTestExecutionException] {
      mutantRunner.runMutationTestProcess(metaMutantProgramSource, equivalentMutants, false, List())
    }
  }
}