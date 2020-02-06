package br.ufrn.dimap.forall.transmut.report.json.model

case class ProgramJSON(
  id:                       Long,
  programSourceId:          Long,
  name:                     String,
  code:                     String,
  datasets:                 List[DatasetJSON],
  transformations:          List[TransformationJSON],
  edges:                    List[EdgeJSON],
  mutants:                  List[MutantJSON],
  mutationOperatorsMetrics: MutationOperatorsJSON,
  totalDatasets:            Int,
  totalTransformations:     Int,
  totalMutants:             Int,
  totalKilledMutants:       Int,
  totalSurvivedMutants:     Int,
  totalEquivalentMutants:   Int,
  totalErrorMutants:        Int,
  mutationScore:            Float)

case class DatasetJSON(
  id:            Long,
  name:          String,
  datasetType:   String,
  inputDataset:  Boolean,
  outputDataset: Boolean)

case class TransformationJSON(
  id:                 Long,
  name:               String,
  inputTypes:         List[String],
  outputTypes:        List[String],
  loadTransformation: Boolean)

case class EdgeJSON(
  id:               Long,
  datasetId:        Long,
  transformationId: Long,
  direction:        String)