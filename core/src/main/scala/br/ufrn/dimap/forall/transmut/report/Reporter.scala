package br.ufrn.dimap.forall.transmut.report

import scala.concurrent.duration.Duration
import scala.concurrent.duration.MILLISECONDS

import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import java.time.LocalDateTime
import java.sql.Timestamp
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved

trait Reporter {

  var _processStartDateTime = LocalDateTime.now()
  var _processStartTime = 0l
  var _processEndTime = 0l
  
  def processStartDateTime = _processStartDateTime

  def processStartDateTime_=(dateTime: LocalDateTime) = {
    _processStartDateTime = dateTime
  }

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
  def onMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource], removedMutants: List[MutantRemoved] = Nil): Unit
  def onMutantExecutionStart(): Unit
  def onMutantExecutionEnd(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]): Unit
  def onProcessEnd(): Unit
  def onAdditionalInformation(msg: String) {}

  def reportProcessStart(startDateTime: LocalDateTime) {
    processStartDateTime = startDateTime
    processStartTime = System.currentTimeMillis()
    onProcessStart
  }

  def reportProcessStart: Unit = reportProcessStart(LocalDateTime.now())

  def reportProgramBuildStart {
    onProgramBuildStart
  }

  def reportProgramBuildEnd(programSources: List[ProgramSource]) {
    onProgramBuildEnd(programSources)
  }

  def reportMutantGenerationStart {
    onMutantGenerationStart
  }

  def reportMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource], removedMutants: List[MutantRemoved] = Nil) {
    onMutantGenerationEnd(metaMutants, removedMutants)
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