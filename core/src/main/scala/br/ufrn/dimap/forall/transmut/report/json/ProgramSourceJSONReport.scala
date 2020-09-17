package br.ufrn.dimap.forall.transmut.report.json

import br.ufrn.dimap.forall.transmut.report.json.MutantJSONReport._
import br.ufrn.dimap.forall.transmut.report.json.RemovedMutantJSONReport._
import br.ufrn.dimap.forall.transmut.report.json.MutationOperatorsJSONReport._
import br.ufrn.dimap.forall.transmut.report.json.ProgramJSONReport._
import br.ufrn.dimap.forall.transmut.report.json.model.EdgeJSON
import br.ufrn.dimap.forall.transmut.report.json.model.ProgramSourceJSON
import br.ufrn.dimap.forall.transmut.report.metric.MetaMutantProgramSourceMetrics
import br.ufrn.dimap.forall.transmut.util.IOFiles

import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.parser.decode

import java.io.File

object ProgramSourceJSONReport {

  implicit val programSourceJSONDecoder: Decoder[ProgramSourceJSON] = deriveDecoder[ProgramSourceJSON]
  implicit val programSourceJSONEncoder: Encoder[ProgramSourceJSON] = deriveEncoder[ProgramSourceJSON]

  def readProgramSourceJSONReportFile(file: File) = {
    val jsonContent = IOFiles.readContentFromFile(file)
    programSourceJSONObjectFromJSONString(jsonContent)
  }
  
  def generateProgramSourceJSONReportFile(directory: File, fileName: String, metrics: MetaMutantProgramSourceMetrics) {
    val content = generateProgramSourceJSONFromMetrics(metrics)
    IOFiles.generateFileWithContent(directory, fileName, content.toString())
  }

  def generateProgramSourceJSONObjectFromMetrics(metrics: MetaMutantProgramSourceMetrics): ProgramSourceJSON = {
    ProgramSourceJSON(
      metrics.id,
      metrics.source.toString,
      metrics.sourceName,
      metrics.metaMutantProgramsMetrics.map(ProgramJSONReport.generateProgramJSONObjectFromMetrics),
      metrics.mutantProgramsMetrics.map(MutantJSONReport.generateMutantJSONObjectFromMetrics),
      metrics.removedMutantsMetrics.map(RemovedMutantJSONReport.generateRemovedMutantJSONObjectFromMetrics),
      MutationOperatorsJSONReport.generateMutationOperatorsJSONObjectFromMetrics(metrics.mutationOperatorsMetrics),
      metrics.totalPrograms,
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

  def generateProgramSourceJSONFromMetrics(metrics: MetaMutantProgramSourceMetrics) = {
    generateProgramSourceJSONObjectFromMetrics(metrics).asJson
  }

  def programSourceJSONObjectFromJSONString(json: String): ProgramSourceJSON = {
    decode[ProgramSourceJSON](json).toOption.get
  }

  def programSourceJSONObjectFromJSONS(json: Json): ProgramSourceJSON = {
    json.as[ProgramSourceJSON].toOption.get
  }

}