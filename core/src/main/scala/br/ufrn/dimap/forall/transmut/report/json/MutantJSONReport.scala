package br.ufrn.dimap.forall.transmut.report.json

import br.ufrn.dimap.forall.transmut.report.metric.MutantProgramMetrics
import br.ufrn.dimap.forall.transmut.report.json.model.MutantJSON
import br.ufrn.dimap.forall.transmut.util.IOFiles

import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.parser.decode

import java.io.File

object MutantJSONReport {

  implicit val mutantJSONDecoder: Decoder[MutantJSON] = deriveDecoder[MutantJSON]
  implicit val mutantJSONEncoder: Encoder[MutantJSON] = deriveEncoder[MutantJSON]

  def readMutantJSONReportFile(file: File) = {
    val jsonContent = IOFiles.readContentFromFile(file)
    mutantJSONObjectFromJSONString(jsonContent)
  }

  def generateMutantJSONReportFile(directory: File, fileName: String, metrics: MutantProgramMetrics) {
    val content = generateMutantJSONFromMetrics(metrics)
    IOFiles.generateFileWithContent(directory, fileName, content.toString())
  }

  def generateMutantJSONObjectFromMetrics(metrics: MutantProgramMetrics): MutantJSON = {
    MutantJSON(
      metrics.mutantId,
      metrics.originalProgramId,
      metrics.mutationOperatorName,
      metrics.mutationOperatorDescription,
      metrics.mutantCode,
      metrics.originalCode,
      metrics.status)
  }

  def generateMutantJSONFromMetrics(metrics: MutantProgramMetrics) = {
    generateMutantJSONObjectFromMetrics(metrics).asJson
  }

  def mutantJSONObjectFromJSONString(json: String): MutantJSON = {
    decode[MutantJSON](json).toOption.get
  }

  def mutantJSONObjectFromJSON(json: Json): MutantJSON = {
    json.as[MutantJSON].toOption.get
  }

}