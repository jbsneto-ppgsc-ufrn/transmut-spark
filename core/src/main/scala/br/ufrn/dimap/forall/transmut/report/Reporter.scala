package br.ufrn.dimap.forall.transmut.report

import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.runner.TestResult
import br.ufrn.dimap.forall.transmut.report.metric.TimeMetrics
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult

trait Reporter {

  protected var processStartTime = System.currentTimeMillis()
  protected var programBuildStartTime = System.currentTimeMillis()
  protected var programBuildEndTime = System.currentTimeMillis()
  protected var mutantGenerationStartTime = System.currentTimeMillis()
  protected var mutantGenerationEndTime = System.currentTimeMillis()
  protected var mutantExecutionStartTime = System.currentTimeMillis()
  protected var mutantExecutionEndTime = System.currentTimeMillis()
  protected var processEndTime = System.currentTimeMillis()

  def onProcessStart() {}
  def onProgramBuildStart() {}
  def onProgramBuildEnd(programSources: List[ProgramSource]) {}
  def onMutantGenerationStart() {}
  def onMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource]) {}
  def onMutantExecutionStart() {}
  def onMutantExecutionEnd(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) {}
  def onProcessEnd() {}

  def reportProcessStart {
    processStartTime = System.currentTimeMillis()
    onProcessStart
  }

  def reportProgramBuildStart {
    programBuildStartTime = System.currentTimeMillis()
    onProgramBuildStart
  }

  def reportProgramBuildEnd(programSources: List[ProgramSource]) {
    programBuildEndTime = System.currentTimeMillis()
    onProgramBuildEnd(programSources)
  }

  def reportMutantGenerationStart {
    mutantGenerationStartTime = System.currentTimeMillis()
    onMutantGenerationStart
  }

  def reportMutantGenerationEnd(metaMutants: List[MetaMutantProgramSource]) {
    mutantGenerationEndTime = System.currentTimeMillis()
    onMutantGenerationEnd(metaMutants)
  }

  def reportMutantExecutionStart {
    mutantExecutionStartTime = System.currentTimeMillis()
    onMutantExecutionStart
  }

  def reportMutantExecutionEnd(metaMutantsVerdicts: List[(MetaMutantProgramSource, List[MutantResult[MutantProgramSource]])]) {
    mutantExecutionEndTime = System.currentTimeMillis()
    onMutantExecutionEnd(metaMutantsVerdicts)
  }

  def reportProcessEnd {
    processEndTime = System.currentTimeMillis()
    onProcessEnd
  }

  def timeMetrics = TimeMetrics(
    processStartTime,
    programBuildStartTime,
    programBuildEndTime,
    mutantGenerationStartTime,
    mutantGenerationEndTime,
    mutantExecutionStartTime,
    mutantExecutionEndTime,
    processEndTime)

}