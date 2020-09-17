package br.ufrn.dimap.forall.transmut.report.html

import br.ufrn.dimap.forall.transmut.config.Config
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.report.Reporter
import br.ufrn.dimap.forall.transmut.report.metric.MutationTestingProcessMetrics
import br.ufrn.dimap.forall.transmut.util.IOFiles
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved

object HTMLReporter extends Reporter {

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
  private var removedMutants: List[MutantRemoved] = List()
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

  override def onMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource], removedMutants: List[MutantRemoved] = Nil) {
    this.removedMutants = removedMutants
    this.metaMutants = metaMutants
  }

  override def onMutantExecutionStart() {}

  override def onMutantExecutionEnd(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) {
    this.metaMutantsVerdicts = metaMutantsVerdicts
  }

  override def onProcessEnd() {
    val mutationTestingProcessMetrics = MutationTestingProcessMetrics(metaMutantsVerdicts, processDuration, processStartDateTime, removedMutants)
    generateMutationTestingProcessReport(mutationTestingProcessMetrics)
    generateProgramSourceReports(mutationTestingProcessMetrics)
    generateProgramReports(mutationTestingProcessMetrics)
    generateMutantReports(mutationTestingProcessMetrics)
    generateRemovedMutantReports(mutationTestingProcessMetrics)
    info("HTML reports generated in " + config.transmutHtmlReportsDir.toString())
  }

  def generateMutationTestingProcessReport(mutationTestingProcessMetrics: MutationTestingProcessMetrics) {
    MutationTestingProcessHTMLReport.generateMutationTestingProcessHtmlReportFile(config.transmutHtmlReportsDir.toFile(), "index.html", mutationTestingProcessMetrics)
  }

  def generateProgramSourceReports(mutationTestingProcessMetrics: MutationTestingProcessMetrics) {
    val programSourcesDir = IOFiles.generateDirectory(config.transmutHtmlReportsDir.toFile(), "ProgramSources")
    mutationTestingProcessMetrics.metaMutantProgramSourcesMetrics.foreach { metrics =>
      ProgramSourceHTMLReport.generateProgramSourceHtmlReportFile(programSourcesDir, s"Program-Source-${metrics.id}.html", metrics)
    }
  }

  def generateProgramReports(mutationTestingProcessMetrics: MutationTestingProcessMetrics) {
    val programsDir = IOFiles.generateDirectory(config.transmutHtmlReportsDir.toFile(), "Programs")
    mutationTestingProcessMetrics.metaMutantProgramsMetrics.foreach { metrics =>
      ProgramHTMLReport.generateProgramHtmlReportFile(programsDir, s"Program-${metrics.id}.html", metrics)
    }
  }

  def generateMutantReports(mutationTestingProcessMetrics: MutationTestingProcessMetrics) {
    val mutantsDir = IOFiles.generateDirectory(config.transmutHtmlReportsDir.toFile(), "Mutants")
    mutationTestingProcessMetrics.mutantProgramsMetrics.foreach { metrics =>
      MutantHTMLReport.generateMutantHtmlReportFile(mutantsDir, s"Mutant-${metrics.mutantId}.html", metrics)
    }
  }

  def generateRemovedMutantReports(mutationTestingProcessMetrics: MutationTestingProcessMetrics) {
    val mutantsDir = IOFiles.generateDirectory(config.transmutHtmlReportsDir.toFile(), "RemovedMutants")
    mutationTestingProcessMetrics.removedMutantsMetrics.foreach { metrics =>
      RemovedMutantHTMLReport.generateRemovedMutantHtmlReportFile(mutantsDir, s"Removed-Mutant-${metrics.mutantId}.html", metrics)
    }
  }

}