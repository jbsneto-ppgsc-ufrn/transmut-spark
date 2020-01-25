package br.ufrn.dimap.forall.transmut.report

import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.report.metric.ProgramSourceMetrics
import br.ufrn.dimap.forall.transmut.report.metric.MetaMutantProgramSourceMetrics
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.report.metric.MutationTestingProcessMetrics

object ConsoleReporter extends Reporter {

  private var programSources: List[ProgramSource] = List()
  private var metaMutants: List[MetaMutantProgramSource] = List()
  private var metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])] = List()

  def info(msg: String) = println(msg)

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
    info("------------------------------------------------------------------------------------")
    info("MUTATION TESTING PROCESS RESULTS")
    info("------------------------------------------------------------------------------------")
    timeMetricsReport()
    programSourcesReport(programSources)
    metaMutantsReport(metaMutants)
    mutationTestingProcessReport(metaMutantsVerdicts)
  }

  def timeMetricsReport() {
    val metrics = timeMetrics
    info("------------------------------------------------------------------------------------")
    info("Program Sources Build Duration: " + metrics.processDuration.toSeconds + " seconds")
    info("Mutants Generation Duration: " + metrics.processDuration.toSeconds + " seconds")
    info("Mutants Tests Duration: " + metrics.processDuration.toSeconds + " seconds")
    info("Mutation Testing Process Total Duration: " + metrics.processDuration.toSeconds + " seconds")
    info("------------------------------------------------------------------------------------")
  }

  def programSourcesReport(programSources: List[ProgramSource]) {
    info("Program Sources Metrics: ")
    programSources.foreach { programSource =>
      val metrics = ProgramSourceMetrics(programSource)
      info("------------------------------------------------------------------------------------")
      info("Program Source: " + programSource.source.getFileName.toString())
      info("Number of Programs: " + metrics.numPrograms)
      info("Programs: ")
      metrics.programsMetrics.foreach { metric =>
        info("Program: " + metric.program.name)
        info("Number of Datasets: " + metric.numDatasets)
        info("Number of Transformations: " + metric.numTransformations)
      }
      info("------------------------------------------------------------------------------------")
    }
  }

  def metaMutantsReport(metaMutants: List[MetaMutantProgramSource]) {
    info("Mutant Metrics: ")
    metaMutants.foreach { metaMutant =>
      val metrics = MetaMutantProgramSourceMetrics(metaMutant)
      info("------------------------------------------------------------------------------------")
      info("Program Source: " + metaMutant.mutated.source.getFileName.toString())
      info("Total Number of Mutants: " + metrics.numMutants)
      metrics.metaMutantProgramsMetrics.foreach { metric =>
        info("Program: " + metric.metaMutant.original.name)
        info("Number of Mutants: " + metric.numMutants)
        info("Number of Mutants For Each Mutation Operator: ")
        metric.numMutantsPerOperator.foreach { op =>
          info(MutationOperatorsEnum.mutationOperatorsNameFromEnum(op._1) + ": " + op._2)
        }
      }
      info("------------------------------------------------------------------------------------")
    }
  }

  def mutationTestingProcessReport(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) {
    info("Mutation Testing Results: ")
    metaMutantsVerdicts.foreach { metaMutantsVerdict =>
      val metaMutant = metaMutantsVerdict._1
      val mutantsResults = metaMutantsVerdict._2
      val metrics = MutationTestingProcessMetrics(metaMutant, mutantsResults)
      info("------------------------------------------------------------------------------------")
      info("Program Source: " + metaMutant.mutated.source.getFileName.toString())
      info("Total Number of Mutants: " + metrics.metaMutantMetrics.numMutants)
      info("Number of Killed Mutants: " + metrics.numKilledMutants)
      info("Number of Survived Mutants: " + metrics.numSurvivedMutants)
      info("Number of Equivalent Mutants: " + metrics.numEquivalentMutants)
      info("Number of Error Mutants: " + metrics.numErrorMutants)
      info("Mutation Score: " + metrics.mutationScore)
      info("List of Killed Mutants IDs: " + metrics.killedMutants.map(_.id).mkString(", "))
      info("List of Survived Mutants IDs: " + metrics.survivedMutants.map(_.id).mkString(", "))
      info("List of Equivalent Mutants IDs: " + metrics.equivalentMutants.map(_.id).mkString(", "))
      info("List of Error Mutants IDs: " + metrics.errorMutants.map(_.id).mkString(", "))
    }
    info("------------------------------------------------------------------------------------")
  }

}