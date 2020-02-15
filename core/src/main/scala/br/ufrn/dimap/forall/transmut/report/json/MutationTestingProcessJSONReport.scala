package br.ufrn.dimap.forall.transmut.report.json

import br.ufrn.dimap.forall.transmut.report.json.MutantJSONReport._
import br.ufrn.dimap.forall.transmut.report.json.MutationOperatorsJSONReport._
import br.ufrn.dimap.forall.transmut.report.json.ProgramJSONReport._
import br.ufrn.dimap.forall.transmut.report.json.ProgramSourceJSONReport._
import br.ufrn.dimap.forall.transmut.report.json.model.EdgeJSON
import br.ufrn.dimap.forall.transmut.report.json.model.ProgramSourceJSON
import br.ufrn.dimap.forall.transmut.report.metric.MetaMutantProgramSourceMetrics
import br.ufrn.dimap.forall.transmut.report.metric.MutationTestingProcessMetrics
import br.ufrn.dimap.forall.transmut.report.json.model.MutationTestingProcessJSON

import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.parser.decode
import br.ufrn.dimap.forall.transmut.util.IOFiles
import java.io.File
import java.time.format.DateTimeFormatter

object MutationTestingProcessJSONReport {

  implicit val mutationTestingProcessJSONDecoder: Decoder[MutationTestingProcessJSON] = deriveDecoder[MutationTestingProcessJSON]
  implicit val mutationTestingProcessJSONEncoder: Encoder[MutationTestingProcessJSON] = deriveEncoder[MutationTestingProcessJSON]

  def readMutationTestingProcessJSONReportFile(file: File) = {
    val jsonContent = IOFiles.readContentFromFile(file)
    mutationTestingProcessJSONObjectFromJSONString(jsonContent)
  }

  def generateMutationTestingProcessJSONReportFile(directory: File, fileName: String, metrics: MutationTestingProcessMetrics) {
    val content = generateProgramSourceJSONFromMetrics(metrics)
    IOFiles.generateFileWithContent(directory, fileName, content.toString())
  }

  def generateProgramSourceJSONObjectFromMetrics(metrics: MutationTestingProcessMetrics): MutationTestingProcessJSON = {
    MutationTestingProcessJSON(
      metrics.processStartDateTime.format(DateTimeFormatter.ISO_DATE_TIME),
      metrics.processDuration.toSeconds,
      metrics.metaMutantProgramSourcesMetrics.map(ProgramSourceJSONReport.generateProgramSourceJSONObjectFromMetrics),
      metrics.metaMutantProgramsMetrics.map(ProgramJSONReport.generateProgramJSONObjectFromMetrics),
      metrics.mutantProgramsMetrics.map(MutantJSONReport.generateMutantJSONObjectFromMetrics),
      MutationOperatorsJSONReport.generateMutationOperatorsJSONObjectFromMetrics(metrics.mutationOperatorsMetrics),
      metrics.totalMetaMutanProgramSources,
      metrics.totalMetaMutantPrograms,
      metrics.totalDatasets,
      metrics.totalTransformations,
      metrics.totalMutants,
      metrics.totalKilledMutants,
      metrics.totalSurvivedMutants,
      metrics.totalEquivalentMutants,
      metrics.totalErrorMutants,
      metrics.totalMutationScore)
  }

  def generateProgramSourceJSONFromMetrics(metrics: MutationTestingProcessMetrics) = {
    generateProgramSourceJSONObjectFromMetrics(metrics).asJson
  }

  def mutationTestingProcessJSONObjectFromJSONString(json: String): MutationTestingProcessJSON = {
    decode[MutationTestingProcessJSON](json).toOption.get
  }

  def mutationTestingProcessJSONObjectFromJSONS(json: Json): MutationTestingProcessJSON = {
    json.as[MutationTestingProcessJSON].toOption.get
  }

}