package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.mutation.model.MetaMutantProgram
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

case class MetaMutantProgramMetrics(metaMutant: MetaMutantProgram) {
  def numMutants = metaMutant.mutants.size
  def numMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (op, metaMutant.mutants.filter(m => m.mutationOperator == op).size)).toMap
}