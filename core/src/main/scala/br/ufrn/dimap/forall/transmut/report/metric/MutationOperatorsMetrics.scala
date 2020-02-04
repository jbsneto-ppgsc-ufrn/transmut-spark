package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

case class MutationOperatorsMetrics(mutantsMetrics: List[MutantProgramMetrics]) {
  
  def totalMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), mutantsMetrics.filter(m => m.mutationOperator == op).size)).toMap

  def totalKilledMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), mutantsMetrics.filter(m => m.mutationOperator == op && m.status == "Killed").size)).toMap

  def totalSurvivedMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), mutantsMetrics.filter(m => m.mutationOperator == op && m.status == "Survived").size)).toMap

  def totalEquivalentMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), mutantsMetrics.filter(m => m.mutationOperator == op && m.status == "Equivalent").size)).toMap

  def totalErrorMutantsPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), mutantsMetrics.filter(m => m.mutationOperator == op && m.status == "Error").size)).toMap
  
  def descriptionPerOperator = MutationOperatorsEnum.ALL.map(op => (MutationOperatorsEnum.mutationOperatorsNameFromEnum(op), MutationOperatorsEnum.mutationOperatorsDescription(op))).toMap
  
}