package br.ufrn.dimap.forall.transmut.report.json.model

case class MutationTestingProcessJSON(
  processStartDateTime:     String, // ISO DATE TIME String Format
  processDurationSeconds:   Long,
  programSources:           List[ProgramSourceJSON],
  programs:                 List[ProgramJSON],
  mutants:                  List[MutantJSON],
  removedMutants:           List[RemovedMutantJSON],
  mutationOperatorsMetrics: MutationOperatorsJSON,
  totalProgramSources:      Int,
  totalPrograms:            Int,
  totalDatasets:            Int,
  totalTransformations:     Int,
  totalMutants:             Int,
  totalKilledMutants:       Int,
  totalLivedMutants:        Int,
  totalEquivalentMutants:   Int,
  totalErrorMutants:        Int,
  totalRemovedMutants:      Int,
  totalMutationScore:       Float)