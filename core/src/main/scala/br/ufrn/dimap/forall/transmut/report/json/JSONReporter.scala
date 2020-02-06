package br.ufrn.dimap.forall.transmut.report.json

import br.ufrn.dimap.forall.transmut.config.Config
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.report.Reporter
import br.ufrn.dimap.forall.transmut.report.metric.MutationTestingProcessMetrics
import br.ufrn.dimap.forall.transmut.util.IOFiles

object JSONReporter extends Reporter {

  def apply(inf: String => Unit)(implicit config: Config) = {
    this.info = inf
    this.config = config
    this
  }

  def apply()(implicit config: Config) = {
    this.config = config
    this
  }

  private var programSources: List[ProgramSource] = List()
  private var metaMutants: List[MetaMutantProgramSource] = List()
  private var metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])] = List()
  private var info: String => Unit = println(_)
  private implicit var config: Config = _

  override def onProcessStart {}

  override def onProgramBuildStart {}

  override def onProgramBuildEnd(programSources: List[ProgramSource]) {
    this.programSources = programSources
  }

  override def onMutantGenerationStart() {}

  override def onMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource]) {
    this.metaMutants = metaMutants
  }

  override def onMutantExecutionStart() {}

  override def onMutantExecutionEnd(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) {
    this.metaMutantsVerdicts = metaMutantsVerdicts
  }

  override def onProcessEnd() {
    val mutationTestingProcessMetrics = MutationTestingProcessMetrics(metaMutantsVerdicts, processDuration)
    generateMutationTestingProcessReport(mutationTestingProcessMetrics)
    generateProgramSourceReports(mutationTestingProcessMetrics)
    generateProgramReports(mutationTestingProcessMetrics)
    generateMutantReports(mutationTestingProcessMetrics)
    info("JSON reports generated in " + config.transmutJSONReportsDir.toString())
  }

  def generateProgramSourceReports(mutationTestingProcessMetrics: MutationTestingProcessMetrics) {
    val programSourcesDir = IOFiles.generateDirectory(config.transmutJSONReportsDir.toFile(), "ProgramSources")
    mutationTestingProcessMetrics.metaMutantProgramSourcesMetrics.foreach { metrics =>
      ProgramSourceJSONReport.generateProgramSourceJSONReportFile(programSourcesDir, s"Program-Source-${metrics.id}.json", metrics)
    }
  }

  def generateProgramReports(mutationTestingProcessMetrics: MutationTestingProcessMetrics) {
    val programsDir = IOFiles.generateDirectory(config.transmutJSONReportsDir.toFile(), "Programs")
    mutationTestingProcessMetrics.metaMutantProgramsMetrics.foreach { metrics =>
      ProgramJSONReport.generateProgramJSONReportFile(programsDir, s"Program-${metrics.id}.json", metrics)
    }
  }

  def generateMutantReports(mutationTestingProcessMetrics: MutationTestingProcessMetrics) {
    val mutantsDir = IOFiles.generateDirectory(config.transmutJSONReportsDir.toFile(), "Mutants")
    mutationTestingProcessMetrics.mutantProgramsMetrics.foreach { metrics =>
      MutantJSONReport.generateMutantJSONReportFile(mutantsDir, s"Mutant-${metrics.mutantId}.json", metrics)
    }
  }

  def generateMutationTestingProcessReport(mutationTestingProcessMetrics: MutationTestingProcessMetrics) {
    MutationTestingProcessJSONReport.generateMutationTestingProcessJSONReportFile(config.transmutJSONReportsDir.toFile(), "Mutation-Testing-Process.json", mutationTestingProcessMetrics)
  }

}