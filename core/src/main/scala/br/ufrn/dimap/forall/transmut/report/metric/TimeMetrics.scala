package br.ufrn.dimap.forall.transmut.report.metric

import scala.concurrent.duration.{Duration, MILLISECONDS}

case class TimeMetrics(
  processStartTime:          Long,
  programBuildStartTime:     Long,
  programBuildEndTime:       Long,
  mutantGenerationStartTime: Long,
  mutantGenerationEndTime:   Long,
  mutantExecutionStartTime:  Long,
  mutantExecutionEndTime:    Long,
  processEndTime:            Long) {
  
  def processDuration = Duration(processEndTime - processStartTime, MILLISECONDS)
  def programBuildDuration = Duration(programBuildEndTime - programBuildStartTime, MILLISECONDS)
  def mutantGenerationDuration = Duration(mutantGenerationEndTime - mutantGenerationStartTime, MILLISECONDS)
  def mutantExecutionDuration = Duration(mutantExecutionEndTime - mutantExecutionStartTime, MILLISECONDS)

}