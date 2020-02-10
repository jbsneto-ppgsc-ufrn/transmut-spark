package br.ufrn.dimap.forall.transmut.report

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
import br.ufrn.dimap.forall.transmut.mutation.runner.TestResult
import br.ufrn.dimap.forall.transmut.mutation.runner.MutantRunner
import br.ufrn.dimap.forall.transmut.mutation.runner.TestSuccess
import br.ufrn.dimap.forall.transmut.mutation.runner.TestError
import br.ufrn.dimap.forall.transmut.mutation.runner.TestFailed
import br.ufrn.dimap.forall.transmut.analyzer.ProgramBuilder
import br.ufrn.dimap.forall.transmut.config.Config
import br.ufrn.dimap.forall.transmut.mutation.manager.MutationManager
import br.ufrn.dimap.forall.transmut.mutation.manager.MetaMutantBuilder
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantAnalyzer
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.process.MutationTestingProcess
import java.nio.file.Files
import java.nio.file.Paths
import org.scalatest.BeforeAndAfter
import br.ufrn.dimap.forall.transmut.util.IOFiles
import br.ufrn.dimap.forall.transmut.report.html.HTMLReporter
import br.ufrn.dimap.forall.transmut.report.json.JSONReporter

class CompoundReporterTestSuite extends FunSuite with BeforeAndAfter with MockFactory {

  val transmutTestDir = Paths.get("./bin/transmut/")

  before {
    if (Files.exists(transmutTestDir) && Files.isRegularFile(transmutTestDir)) {
      IOFiles.deleteFile(transmutTestDir.toFile())
    }
  }

  test("Tast Case 1 - Mutation Testing Process With Compound Reporters (Console, HTML and JSON)") {

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

    val programSourcesNames = List("SparkProgram.scala")
    val programNames = List("program")
    val mutationOperators = List("JTR")

    implicit val configTest = Config(programSourcesNames, programNames, mutationOperators, transmutDir = transmutTestDir)

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(configTest.programs, tree, refenceTypes.toMap)
    val programSources = List(programSource)
    val programBuilderMock = mock[ProgramBuilder] // Using a mock to not generate the program source from a file
    (programBuilderMock.buildProgramSources _).expects(configTest.sources, configTest.programs, configTest.transmutSrcDir, configTest.semanticdbDir).returning(programSources).once()

    val defaultMutantsIdGenerator = LongIdGenerator.generator
    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(JTR), defaultMutantsIdGenerator)

    val mutantManagerMock = mock[MutationManager]
    (mutantManagerMock.defaultMutantsIdGenerator _).expects().returning(defaultMutantsIdGenerator)
    (mutantManagerMock.generateMutantsFromProgramSource(_: ProgramSource, _: List[MutationOperatorsEnum], _: LongIdGenerator)).expects(programSource, configTest.mutationOperatorsList, *).returning(programSourceMutants).once()

    val metaMutantProgramSource = SparkRDDMetaMutantBuilder.buildMetaMutantProgramSourceFromMutantProgramSources(programSource, programSourceMutants)
    val metaMutantProgramSources = List(metaMutantProgramSource)

    val metaMutantBuilderMock = mock[MetaMutantBuilder]
    (metaMutantBuilderMock.buildMetaMutantProgramSourceFromMutantProgramSources _).expects(programSource, programSourceMutants).returning(metaMutantProgramSource)
    (metaMutantBuilderMock.writeMetaMutantToFile(_: MetaMutantProgramSource)).expects(metaMutantProgramSource)

    val mutantJTR1 = metaMutantProgramSource.mutants(0)
    val mutantJTR2 = metaMutantProgramSource.mutants(1)
    val mutantJTR3 = metaMutantProgramSource.mutants(2)
    val metaMutantsTestResult = (metaMutantProgramSource, List(TestFailed(mutantJTR1), TestError(mutantJTR2), TestSuccess(mutantJTR3)))

    val mutantRunnerMock = mock[MutantRunner]
    (mutantRunnerMock.runMutationTestProcess _).expects(metaMutantProgramSource, configTest.equivalentMutants, configTest.isTestOnlyLivingMutants, configTest.livingMutants).returning(metaMutantsTestResult)

    val mutantsVerdicts = List(MutantAnalyzer.analyzeMutants(metaMutantsTestResult._1, metaMutantsTestResult._2, configTest.equivalentMutants))

    val reporterMock = mock[Reporter]
    inSequence {
      (reporterMock.reportProcessStart _).expects()
      (reporterMock.reportProgramBuildStart _).expects()
      (reporterMock.reportProgramBuildEnd _).expects(programSources)
      (reporterMock.reportMutantGenerationStart _).expects()
      (reporterMock.reportMutantGenerationEnd _).expects(metaMutantProgramSources)
      (reporterMock.reportMutantExecutionStart _).expects()
      (reporterMock.reportMutantExecutionEnd _).expects(mutantsVerdicts)
      (reporterMock.reportProcessEnd _).expects()
    }

    val infoConsoleMock = mockFunction[String, Unit]
    infoConsoleMock.expects(*).returning().atLeastTwice()

    val infoHTMLMock = mockFunction[String, Unit]
    infoHTMLMock.expects(*).returning().once()

    val infoJSONMock = mockFunction[String, Unit]
    infoJSONMock.expects(*).returning().once()

    val consoleReporter = ConsoleReporter(infoConsoleMock)
    val htmlReporter = HTMLReporter(infoHTMLMock)
    val jsonReporter = JSONReporter(infoJSONMock)

    val compoundReporter = CompoundReporter(reporterMock, consoleReporter, htmlReporter, jsonReporter)

    val mutationTestingProcess = new MutationTestingProcess {
      def config = configTest
      def reporter = compoundReporter
      def programBuilder = programBuilderMock
      def mutantManager = mutantManagerMock
      def metaMutantBuilder = metaMutantBuilderMock
      def mutantRunner = mutantRunnerMock
    }

    mutationTestingProcess.runProcess()

    val transmutReportsDir = Paths.get(transmutTestDir.toString(), "reports")
    val transmutHTMLReportsDir = Paths.get(transmutReportsDir.toString(), "html")
    val transmutJSONReportsDir = Paths.get(transmutReportsDir.toString(), "json")
    val transmutHTMLIndexFile = Paths.get(transmutHTMLReportsDir.toString(), "index.html")
    val transmutJSONMutationTestingProcessFile = Paths.get(transmutJSONReportsDir.toString(), "Mutation-Testing-Process.json")

    assert(Files.exists(transmutTestDir) && Files.isDirectory(transmutTestDir))
    assert(Files.exists(transmutReportsDir) && Files.isDirectory(transmutReportsDir))
    assert(Files.exists(transmutHTMLReportsDir) && Files.isDirectory(transmutHTMLReportsDir))
    assert(Files.exists(transmutJSONReportsDir) && Files.isDirectory(transmutJSONReportsDir))
    assert(Files.exists(transmutHTMLIndexFile) && Files.isRegularFile(transmutHTMLIndexFile))
    assert(Files.exists(transmutJSONMutationTestingProcessFile) && Files.isRegularFile(transmutJSONMutationTestingProcessFile))
  }

  after {
    if (Files.exists(transmutTestDir) && Files.isDirectory(transmutTestDir)) {
      IOFiles.deleteFile(transmutTestDir.toFile())
    }
  }

}