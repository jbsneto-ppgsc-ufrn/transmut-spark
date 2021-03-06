package br.ufrn.dimap.forall.transmut.report.json.model

case class MutationOperatorsJSON(
  totalMutantsPerOperator:           Map[String, Int],
  totalKilledMutantsPerOperator:     Map[String, Int],
  totalLivedMutantsPerOperator:      Map[String, Int],
  totalEquivalentMutantsPerOperator: Map[String, Int],
  totalErrorMutantsPerOperator:      Map[String, Int],
  totalRemovedMutantsPerOperator:    Map[String, Int],
  descriptionPerOperator:            Map[String, String])