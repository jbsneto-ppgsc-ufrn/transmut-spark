package br.ufrn.dimap.forall.transmut.report.json

import br.ufrn.dimap.forall.transmut.report.json.model.RemovedMutantJSON
import br.ufrn.dimap.forall.transmut.report.metric.RemovedMutantMetrics
import br.ufrn.dimap.forall.transmut.util.IOFiles

import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.parser.decode

import java.io.File

object RemovedMutantJSONReport {

  implicit val removedMutantJSONDecoder: Decoder[RemovedMutantJSON] = deriveDecoder[RemovedMutantJSON]
  implicit val removedMutantJSONEncoder: Encoder[RemovedMutantJSON] = deriveEncoder[RemovedMutantJSON]

  def readRemovedMutantJSONReportFile(file: File) = {
    val jsonContent = IOFiles.readContentFromFile(file)
    removedMutantJSONObjectFromJSONString(jsonContent)
  }

  def generateRemovedMutantJSONReportFile(directory: File, fileName: String, metrics: RemovedMutantMetrics) {
    val content = generateRemovedMutantJSONFromMetrics(metrics)
    IOFiles.generateFileWithContent(directory, fileName, content.toString())
  }

  def generateRemovedMutantJSONObjectFromMetrics(metrics: RemovedMutantMetrics): RemovedMutantJSON = {
    RemovedMutantJSON(
      metrics.mutantId,
      metrics.originalProgramId,
      metrics.mutationOperatorName,
      metrics.mutationOperatorDescription,
      metrics.mutantCode,
      metrics.originalCode,
      metrics.reductionRuleName)
  }

  def generateRemovedMutantJSONFromMetrics(metrics: RemovedMutantMetrics) = {
    generateRemovedMutantJSONObjectFromMetrics(metrics).asJson
  }

  def removedMutantJSONObjectFromJSONString(json: String): RemovedMutantJSON = {
    decode[RemovedMutantJSON](json).toOption.get
  }
 
  def removedMutantJSONObjectFromJSON(json: Json): RemovedMutantJSON = {
    json.as[RemovedMutantJSON].toOption.get
  }
}