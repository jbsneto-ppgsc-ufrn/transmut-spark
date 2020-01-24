package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.model.ProgramSource

case class ProgramSourceMetrics(programSource: ProgramSource) {
  def numPrograms = programSource.programs.size
  def programsMetrics = programSource.programs.map(p => ProgramMetrics(p))
}