package br.ufrn.dimap.forall.transmut.report.json.model

import io.circe._, io.circe.generic.semiauto._

case class ProgramSourceJSON(
  id:                       Long,
  source:                   String,
  sourceName:               String,
  programs:                 List[ProgramJSON],
  mutants:                  List[MutantJSON],
  removedMutants:           List[RemovedMutantJSON],
  mutationOperatorsMetrics: MutationOperatorsJSON,
  totalPrograms:            Int,
  totalDatasets:            Int,
  totalTransformations:     Int,
  totalMutants:             Int,
  totalKilledMutants:       Int,
  totalLivedMutants:        Int,
  totalEquivalentMutants:   Int,
  totalErrorMutants:        Int,
  totalRemovedMutants:      Int,
  mutationScore:            Float)