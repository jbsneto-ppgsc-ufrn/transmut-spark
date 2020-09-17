package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

case class MutationOperatorsMetrics(mutantsMetrics: List[MutantProgramMetrics], removedMutantsMetrics: List[RemovedMutantMetrics]) {
  
  def totalMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), mutantsMetrics.filter(m => m.mutationOperator == op).size + removedMutantsMetrics.filter(rm => rm.mutationOperator == op).size)).toMap

  def totalKilledMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), mutantsMetrics.filter(m => m.mutationOperator == op && m.status == "Killed").size)).toMap

  def totalLivedMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), mutantsMetrics.filter(m => m.mutationOperator == op && m.status == "Lived").size)).toMap

  def totalEquivalentMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), mutantsMetrics.filter(m => m.mutationOperator == op && m.status == "Equivalent").size)).toMap

  def totalErrorMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), mutantsMetrics.filter(m => m.mutationOperator == op && m.status == "Error").size)).toMap
  
  def descriptionPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), MutationOperatorsEnum.mutationOperatorsDescription(op))).toMap
  
  def totalRemovedMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), removedMutantsMetrics.filter(rm => rm.mutationOperator == op).size)).toMap
  
}