package br.ufrn.dimap.forall.transmut.report

import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.report.metric.ProgramSourceMetrics
import br.ufrn.dimap.forall.transmut.report.metric.MetaMutantProgramSourceMetrics
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.report.metric.MutationTestingProcessMetrics
import java.time.format.DateTimeFormatter

object ConsoleReporter extends Reporter {

  def apply(inf: String => Unit) = {
    this.info = inf
    this
  }

  private var programSources: List[ProgramSource] = List()
  private var metaMutants: List[MetaMutantProgramSource] = List()
  private var metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])] = List()
  private var info: String => Unit = println(_)

  override def onAdditionalInformation(msg: String) {
    info(msg)
  }

  override def onProcessStart {
    info("Starting the mutation testing process...")
  }

  override def onProgramBuildStart {
    info("Starting the code parsing and program source build...")
  }

  override def onProgramBuildEnd(programSources: List[ProgramSource]) {
    this.programSources = programSources
    info("Code parsing and program source build succeeded!")
  }

  override def onMutantGenerationStart() {
    info("Starting the mutants and meta-mutant generation...")
  }

  override def onMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource]) {
    this.metaMutants = metaMutants
    info("Mutants and meta-mutant generation succeeded!")
  }

  override def onMutantExecutionStart() {
    info("Starting test execution with the original program and mutants...")
  }

  override def onMutantExecutionEnd(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) {
    this.metaMutantsVerdicts = metaMutantsVerdicts
    info("Test execution with the original program and mutants completed!")
  }

  override def onProcessEnd() {
    val mutationTestingProcessMetrics = MutationTestingProcessMetrics(metaMutantsVerdicts, processDuration, processStartDateTime)
    val metaMutantProgramSourcesMetrics = mutationTestingProcessMetrics.metaMutantProgramSourcesMetrics
    info("------------------------------------------------------------------------------------")
    info("MUTATION TESTING PROCESS RESULTS")
    info("------------------------------------------------------------------------------------")
    programSourcesReport(metaMutantProgramSourcesMetrics)
    metaMutantsReport(metaMutantProgramSourcesMetrics)
    mutationTestingProcessReport(metaMutantProgramSourcesMetrics)
    generalMutationTestingProcessReport(mutationTestingProcessMetrics)
  }

  def generalMutationTestingProcessReport(metric: MutationTestingProcessMetrics) {
    info("Mutation Testing Process Start Date: " + metric.processStartDateTime.format(DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")))
    info("------------------------------------------------------------------------------------")
    info("Mutation Testing Process Duration: " + metric.processDuration.toSeconds + " seconds")
    info("------------------------------------------------------------------------------------")
    info("General Mutation Testing Results: ")
    info("------------------------------------------------------------------------------------")
    info("Program Sources: " + metric.totalMetaMutanProgramSources)
    info("Programs: " + metric.totalMetaMutantPrograms)
    info("Total Number of Mutants: " + metric.totalMutants)
    info("Total Number of Killed Mutants: " + metric.totalKilledMutants)
    info("Total Number of Survived Mutants: " + metric.totalSurvivedMutants)
    info("Total Number of Equivalent Mutants: " + metric.totalEquivalentMutants)
    info("Total Number of Error Mutants: " + metric.totalErrorMutants)
    info("General Mutation Score: " + f"${(metric.totalMutationScore * 100)}%.2f%%")
    info("------------------------------------------------------------------------------------")
  }

  def programSourcesReport(metaProgramSourcesMetrics: List[MetaMutantProgramSourceMetrics]) {
    info("Program Sources Metrics: ")
    metaProgramSourcesMetrics.foreach { metrics =>
      info("------------------------------------------------------------------------------------")
      info("Program Source: " + metrics.sourceName)
      info("Number of Programs: " + metrics.totalPrograms)
      info("Programs: ")
      metrics.metaMutantProgramsMetrics.foreach { metric =>
        info("Program: " + metric.name)
        info("Number of Datasets: " + metric.totalDatasets)
        info("Number of Transformations: " + metric.totalTransformations)
      }
      info("------------------------------------------------------------------------------------")
    }
  }

  def metaMutantsReport(metaProgramSourcesMetrics: List[MetaMutantProgramSourceMetrics]) {
    info("Mutant Metrics: ")
    metaProgramSourcesMetrics.foreach { programSourceMetric =>
      info("------------------------------------------------------------------------------------")
      info("Program Source: " + programSourceMetric.sourceName)
      info("Total Number of Mutants: " + programSourceMetric.totalMutants)
      programSourceMetric.metaMutantProgramsMetrics.foreach { programMetric =>
        info("Program: " + programMetric.name)
        info("Number of Mutants: " + programMetric.totalMutants)
        info("Number of Mutants For Each Mutation Operator: ")
        programMetric.mutationOperatorsMetrics.totalMutantsPerOperator.foreach { op =>
          if (op._2 > 0)
            info(op._1 + ": " + op._2)
        }
      }
      info("------------------------------------------------------------------------------------")
    }
  }

  def mutationTestingProcessReport(metaProgramSourcesMetrics: List[MetaMutantProgramSourceMetrics]) {
    info("Mutation Testing Results: ")
    metaProgramSourcesMetrics.foreach { metaMutantProgramSource =>
      info("------------------------------------------------------------------------------------")
      info("Program Source: " + metaMutantProgramSource.sourceName)
      info("Total Number of Mutants: " + metaMutantProgramSource.totalMutants)
      info("Number of Killed Mutants: " + metaMutantProgramSource.totalKilledMutants)
      info("Number of Survived Mutants: " + metaMutantProgramSource.totalSurvivedMutants)
      info("Number of Equivalent Mutants: " + metaMutantProgramSource.totalEquivalentMutants)
      info("Number of Error Mutants: " + metaMutantProgramSource.totalErrorMutants)
      info("Mutation Score: " + f"${(metaMutantProgramSource.mutationScore * 100)}%.2f%%")
      info("List of Killed Mutants IDs: " + metaMutantProgramSource.killedMutants.map(_.id).mkString(", "))
      info("List of Survived Mutants IDs: " + metaMutantProgramSource.survivedMutants.map(_.id).mkString(", "))
      info("List of Equivalent Mutants IDs: " + metaMutantProgramSource.equivalentMutants.map(_.id).mkString(", "))
      info("List of Error Mutants IDs: " + metaMutantProgramSource.errorMutants.map(_.id).mkString(", "))
    }
    info("------------------------------------------------------------------------------------")
  }

}