package br.ufrn.dimap.forall.transmut.report.json

import br.ufrn.dimap.forall.transmut.model.Dataset
import br.ufrn.dimap.forall.transmut.model.DirectionsEnum
import br.ufrn.dimap.forall.transmut.model.DirectionsEnum.DirectionsEnum
import br.ufrn.dimap.forall.transmut.model.Edge
import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.report.json.model.DatasetJSON
import br.ufrn.dimap.forall.transmut.report.json.model.EdgeJSON
import br.ufrn.dimap.forall.transmut.report.json.model.ProgramJSON
import br.ufrn.dimap.forall.transmut.report.json.model.TransformationJSON
import br.ufrn.dimap.forall.transmut.report.metric.MetaMutantProgramMetrics
import br.ufrn.dimap.forall.transmut.report.json.MutantJSONReport._
import br.ufrn.dimap.forall.transmut.report.json.RemovedMutantJSONReport._
import br.ufrn.dimap.forall.transmut.report.json.MutationOperatorsJSONReport._
import br.ufrn.dimap.forall.transmut.util.IOFiles

import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.parser.decode

import java.io.File

object ProgramJSONReport {

  implicit val programJSONDecoder: Decoder[ProgramJSON] = deriveDecoder[ProgramJSON]
  implicit val programJSONEncoder: Encoder[ProgramJSON] = deriveEncoder[ProgramJSON]

  implicit val datasetJSONDecoder: Decoder[DatasetJSON] = deriveDecoder[DatasetJSON]
  implicit val datasetJSONEncoder: Encoder[DatasetJSON] = deriveEncoder[DatasetJSON]

  implicit val transformationJSONDecoder: Decoder[TransformationJSON] = deriveDecoder[TransformationJSON]
  implicit val transformationJSONEncoder: Encoder[TransformationJSON] = deriveEncoder[TransformationJSON]

  implicit val edgeJSONDecoder: Decoder[EdgeJSON] = deriveDecoder[EdgeJSON]
  implicit val edgeJSONEncoder: Encoder[EdgeJSON] = deriveEncoder[EdgeJSON]

  def readProgramJSONReportFile(file: File) = {
    val jsonContent = IOFiles.readContentFromFile(file)
    programJSONObjectFromJSONString(jsonContent)
  }
  
  def generateProgramJSONReportFile(directory: File, fileName: String, metrics: MetaMutantProgramMetrics) {
    val content = generateProgramJSONFromMetrics(metrics)
    IOFiles.generateFileWithContent(directory, fileName, content.toString())
  }

  def generateProgramJSONObjectFromMetrics(metrics: MetaMutantProgramMetrics): ProgramJSON = {
    ProgramJSON(
      metrics.id,
      metrics.programSourceId,
      metrics.name,
      metrics.code,
      metrics.datasets.map(generateDatasetJSONFromDataset),
      metrics.transformations.map(generateTransformationJSONFromTransformation),
      metrics.edges.map(generateEdgeJSONFromEdge),
      metrics.mutantsMetrics.map(MutantJSONReport.generateMutantJSONObjectFromMetrics),
      metrics.removedMutantsMetrics.map(RemovedMutantJSONReport.generateRemovedMutantJSONObjectFromMetrics),
      MutationOperatorsJSONReport.generateMutationOperatorsJSONObjectFromMetrics(metrics.mutationOperatorsMetrics),
      metrics.totalDatasets,
      metrics.totalTransformations,
      metrics.totalMutants,
      metrics.totalKilledMutants,
      metrics.totalLivedMutants,
      metrics.totalEquivalentMutants,
      metrics.totalErrorMutants,
      metrics.totalRemovedMutants,
      metrics.mutationScore)
  }

  def generateProgramJSONFromMetrics(metrics: MetaMutantProgramMetrics) = {
    generateProgramJSONObjectFromMetrics(metrics).asJson
  }

  def programJSONObjectFromJSONString(json: String): ProgramJSON = {
    decode[ProgramJSON](json).toOption.get
  }

  def programJSONObjectFromJSONS(json: Json): ProgramJSON = {
    json.as[ProgramJSON].toOption.get
  }

  private def generateDatasetJSONFromDataset(dataset: Dataset): DatasetJSON = {
    DatasetJSON(
      dataset.id,
      dataset.name,
      dataset.datasetType.simplifiedName,
      dataset.isInputDataset,
      dataset.isOutputDataset)
  }

  private def generateTransformationJSONFromTransformation(transformation: Transformation): TransformationJSON = {
    TransformationJSON(
      transformation.id,
      transformation.name,
      transformation.inputTypes.map(_.simplifiedName),
      transformation.outputTypes.map(_.simplifiedName),
      transformation.isLoadTransformation)
  }

  private def generateEdgeJSONFromEdge(edge: Edge): EdgeJSON = {
    val direction = edge.direction match {
      case DirectionsEnum.DatasetToTransformation => "DatasetToTransformation"
      case DirectionsEnum.TransformationToDataset => "TransformationToDataset"
    }
    EdgeJSON(
      edge.id,
      edge.dataset.id,
      edge.transformation.id,
      direction)
  }

}