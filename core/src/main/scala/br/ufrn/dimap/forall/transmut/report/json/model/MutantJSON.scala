package br.ufrn.dimap.forall.transmut.report.json.model

case class MutantJSON(
  id:                Long,
  originalProgramId: Long,
  mutationOperator:  String,
  mutationOperatorDescription:  String,
  mutantCode:        String,
  originalCode:      String,
  status:            String)