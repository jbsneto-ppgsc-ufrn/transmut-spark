package br.ufrn.dimap.forall.transmut.report

import com.typesafe.scalalogging.LazyLogging

import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.report.metric.ProgramSourceMetrics
import br.ufrn.dimap.forall.transmut.report.metric.MetaMutantProgramSourceMetrics
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.report.metric.MutationTestingProcessMetrics

class ConsoleReporter extends Reporter with LazyLogging {

  private var programSources: List[ProgramSource] = List()
  private var metaMutants: List[MetaMutantProgramSource] = List()
  private var metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])] = List()

  override def onProcessStart {
    logger.info("Starting the mutation testing process...")
  }

  override def onProgramBuildStart {
    logger.info("Starting the code parsing and program source build...")
  }

  override def onProgramBuildEnd(programSources: List[ProgramSource]) {
    this.programSources = programSources
    logger.info("Code parsing and program source build succeeded!")
  }

  override def onMutantGenerationStart() {
    logger.info("Starting the mutants and meta-mutant generation...")
  }

  override def onMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource]) {
    this.metaMutants = metaMutants
    logger.info("Mutants and meta-mutant generation succeeded!")
  }

  override def onMutantExecutionStart() {
    logger.info("Starting test execution with the original program and mutants...")
  }

  override def onMutantExecutionEnd(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) {
    this.metaMutantsVerdicts = metaMutantsVerdicts
    logger.info("Test execution with the original program and mutants completed!")
  }

  override def onProcessEnd() {
    logger.info("------------------------------------------------------------------------------------")
    logger.info("MUTATION TESTING PROCESS RESULTS")
    logger.info("------------------------------------------------------------------------------------")
    timeMetricsReport()
    programSourcesReport(programSources)
    metaMutantsReport(metaMutants)
    mutationTestingProcessReport(metaMutantsVerdicts)
  }

  def timeMetricsReport() {
    val metrics = timeMetrics
    logger.info("------------------------------------------------------------------------------------")
    logger.info("Program Sources Build Duration: " + metrics.processDuration.toSeconds + " seconds")
    logger.info("Mutants Generation Duration: " + metrics.processDuration.toSeconds + " seconds")
    logger.info("Mutants Tests Duration: " + metrics.processDuration.toSeconds + " seconds")
    logger.info("Mutation Testing Process Total Duration: " + metrics.processDuration.toSeconds + " seconds")
    logger.info("------------------------------------------------------------------------------------")
  }

  def programSourcesReport(programSources: List[ProgramSource]) {
    logger.info("Program Sources Metrics: ")
    programSources.foreach { programSource =>
      val metrics = ProgramSourceMetrics(programSource)
      logger.info("------------------------------------------------------------------------------------")
      logger.info("Program Source: " + programSource.source.getFileName.toString())
      logger.info("Number of Programs: " + metrics.numPrograms)
      logger.info("Programs: ")
      metrics.programsMetrics.foreach { metric =>
        logger.info("Program: " + metric.program.name)
        logger.info("Number of Datasets: " + metric.numDatasets)
        logger.info("Number of Transformations: " + metric.numTransformations)
      }
      logger.info("------------------------------------------------------------------------------------")
    }
  }

  def metaMutantsReport(metaMutants: List[MetaMutantProgramSource]) {
    logger.info("Mutant Metrics: ")
    metaMutants.foreach { metaMutant =>
      val metrics = MetaMutantProgramSourceMetrics(metaMutant)
      logger.info("------------------------------------------------------------------------------------")
      logger.info("Program Source: " + metaMutant.mutated.source.getFileName.toString())
      logger.info("Total Number of Mutants: " + metrics.numMutants)
      metrics.metaMutantProgramsMetrics.foreach { metric =>
        logger.info("Program: " + metric.metaMutant.original.name)
        logger.info("Number of Mutants: " + metric.numMutants)
        logger.info("Number of Mutants For Each Mutation Operator: ")
        metric.numMutantsPerOperator.foreach { op =>
          logger.info(MutationOperatorsEnum.mutationOperatorsNameFromEnum(op._1) + ": " + op._2)
        }
      }
      logger.info("------------------------------------------------------------------------------------")
    }
  }

  def mutationTestingProcessReport(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) {
    logger.info("Mutation Testing Results: ")
    metaMutantsVerdicts.foreach { metaMutantsVerdict =>
      val metaMutant = metaMutantsVerdict._1
      val mutantsResults = metaMutantsVerdict._2
      val metrics = MutationTestingProcessMetrics(metaMutant, mutantsResults)
      logger.info("------------------------------------------------------------------------------------")
      logger.info("Program Source: " + metaMutant.mutated.source.getFileName.toString())
      logger.info("Total Number of Mutants: " + metrics.metaMutantMetrics.numMutants)
      logger.info("Number of Killed Mutants: " + metrics.numKilledMutants)
      logger.info("Number of Survived Mutants: " + metrics.numSurvivedMutants)
      logger.info("Number of Equivalent Mutants: " + metrics.numEquivalentMutants)
      logger.info("Number of Error Mutants: " + metrics.numErrorMutants)
      logger.info("Mutation Score: " + metrics.mutationScore)
      logger.info("List of Killed Mutants IDs: " + metrics.killedMutants.map(_.id).mkString(", "))
      logger.info("List of Survived Mutants IDs: " + metrics.survivedMutants.map(_.id).mkString(", "))
      logger.info("List of Equivalent Mutants IDs: " + metrics.equivalentMutants.map(_.id).mkString(", "))
      logger.info("List of Error Mutants IDs: " + metrics.errorMutants.map(_.id).mkString(", "))
    }
    logger.info("------------------------------------------------------------------------------------")
  }

}