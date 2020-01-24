package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

case class MetaMutantProgramSourceMetrics(metaMutant: MetaMutantProgramSource) {
  def originalProgramSourceMetrics = ProgramSourceMetrics(metaMutant.original)
  def metaMutantProgramsMetrics = metaMutant.metaMutantPrograms.map(m => MetaMutantProgramMetrics(m))
  def numMutants = metaMutant.mutants.size
  def numMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (op, metaMutant.mutants.filter(m => m.mutationOperator == op).size)).toMap
}