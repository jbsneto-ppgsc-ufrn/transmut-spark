package br.ufrn.dimap.forall.transmut.report

import scala.concurrent.duration.Duration
import scala.concurrent.duration.MILLISECONDS

import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource

trait Reporter {

  var _processStartTime = 0l
  var _processEndTime = 0l

  def processStartTime = _processStartTime

  def processStartTime_=(start: Long) {
    _processStartTime = start
  }

  def processEndTime = _processEndTime

  def processEndTime_=(end: Long) {
    _processEndTime = end
  }

  def onProcessStart(): Unit
  def onProgramBuildStart(): Unit
  def onProgramBuildEnd(programSources: List[ProgramSource]): Unit
  def onMutantGenerationStart(): Unit
  def onMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource]): Unit
  def onMutantExecutionStart(): Unit
  def onMutantExecutionEnd(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]): Unit
  def onProcessEnd(): Unit
  def onAdditionalInformation(msg: String) {}

  def reportProcessStart {
    processStartTime = System.currentTimeMillis()
    onProcessStart
  }

  def reportProgramBuildStart {
    onProgramBuildStart
  }

  def reportProgramBuildEnd(programSources: List[ProgramSource]) {
    onProgramBuildEnd(programSources)
  }

  def reportMutantGenerationStart {
    onMutantGenerationStart
  }

  def reportMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource]) {
    onMutantGenerationEnd(metaMutants)
  }

  def reportMutantExecutionStart {
    onMutantExecutionStart
  }

  def reportMutantExecutionEnd(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) {
    onMutantExecutionEnd(metaMutantsVerdicts)
  }

  def reportProcessEnd {
    processEndTime = System.currentTimeMillis()
    onProcessEnd
  }

  def reportAdditionalInformation(msg: String) {
    onAdditionalInformation(msg)
  }

  def processDuration = Duration(processEndTime - processStartTime, MILLISECONDS)

}