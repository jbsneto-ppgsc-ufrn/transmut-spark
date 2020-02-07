package br.ufrn.dimap.forall.transmut.report.json.model

case class MutationTestingProcessJSON(
  processDurationSeconds:   Long,
  programSources:           List[ProgramSourceJSON],
  programs:                 List[ProgramJSON],
  mutants:                  List[MutantJSON],
  mutationOperatorsMetrics: MutationOperatorsJSON,
  totalProgramSources:      Int,
  totalPrograms:            Int,
  totalDatasets:            Int,
  totalTransformations:     Int,
  totalMutants:             Int,
  totalKilledMutants:       Int,
  totalSurvivedMutants:     Int,
  totalEquivalentMutants:   Int,
  totalErrorMutants:        Int,
  totalMutationScore:       Float)