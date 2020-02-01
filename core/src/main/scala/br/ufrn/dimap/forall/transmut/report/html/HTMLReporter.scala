package br.ufrn.dimap.forall.transmut.report.html

import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.report.metric.MetaMutantProgramSourceMetrics
import br.ufrn.dimap.forall.transmut.report.metric.MutationTestingProcessMetrics
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.report.Reporter
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.config.Config

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
    MutationTestingProcessHTMLReport.generateProgramHtmlReportFile(config.transmutHtmlReportsDir.toFile(), "index.html", mutationTestingProcessMetrics)
    info("HTML reports generated in " + config.transmutHtmlReportsDir.toString())
  }

}