package br.ufrn.dimap.forall.transmut.report

import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import java.time.LocalDateTime

object CompoundReporter extends Reporter {

  var reporters: List[Reporter] = List(ConsoleReporter)

  def apply(reporters: Reporter*) = {
    this.reporters = reporters.toList
    this
  }

  def onProcessStart() = reporters.foreach(reporter => reporter.onProcessStart())
  def onProgramBuildStart() = reporters.foreach(reporter => reporter.onProgramBuildStart())
  def onProgramBuildEnd(programSources: List[ProgramSource]) = reporters.foreach(reporter => reporter.onProgramBuildEnd(programSources))
  def onMutantGenerationStart() = reporters.foreach(reporter => reporter.onMutantGenerationStart())
  def onMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource]) = reporters.foreach(reporter => reporter.onMutantGenerationEnd(metaMutants))
  def onMutantExecutionStart() = reporters.foreach(reporter => reporter.onMutantExecutionStart())
  def onMutantExecutionEnd(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) = reporters.foreach(reporter => reporter.onMutantExecutionEnd(metaMutantsVerdicts))
  def onProcessEnd() = reporters.foreach(reporter => reporter.onProcessEnd())
  override def onAdditionalInformation(msg: String) = reporters.foreach(reporter => reporter.onAdditionalInformation(msg))

  override def reportProcessStart(startDateTime: LocalDateTime) = reporters.foreach(reporter => reporter.reportProcessStart(startDateTime))
  override def reportProcessStart = reporters.foreach(reporter => reporter.reportProcessStart)
  override def reportProgramBuildStart = reporters.foreach(reporter => reporter.reportProgramBuildStart)
  override def reportProgramBuildEnd(programSources: List[ProgramSource]) = reporters.foreach(reporter => reporter.reportProgramBuildEnd(programSources))
  override def reportMutantGenerationStart = reporters.foreach(reporter => reporter.reportMutantGenerationStart)
  override def reportMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource]) = reporters.foreach(reporter => reporter.reportMutantGenerationEnd(metaMutants))
  override def reportMutantExecutionStart = reporters.foreach(reporter => reporter.reportMutantExecutionStart)
  override def reportMutantExecutionEnd(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) = reporters.foreach(reporter => reporter.reportMutantExecutionEnd(metaMutantsVerdicts))
  override def reportProcessEnd = reporters.foreach(reporter => reporter.reportProcessEnd)
  override def reportAdditionalInformation(msg: String) = reporters.foreach(reporter => reporter.reportAdditionalInformation(msg))
}