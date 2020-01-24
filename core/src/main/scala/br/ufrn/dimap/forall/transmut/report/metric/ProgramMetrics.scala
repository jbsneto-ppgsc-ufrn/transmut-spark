package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.model.Program

case class ProgramMetrics(program: Program) {
  def numDatasets = program.datasets.size
  def numTransformations = program.transformations.size
  def numEdges = program.edges.size
}