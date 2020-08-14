package br.ufrn.dimap.forall.transmut.report.json

import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.parser.decode

import br.ufrn.dimap.forall.transmut.report.json.model.MutationOperatorsJSON
import br.ufrn.dimap.forall.transmut.report.metric.MutationOperatorsMetrics

object MutationOperatorsJSONReport {

  implicit val mutationOperatorsJSONDecoder: Decoder[MutationOperatorsJSON] = deriveDecoder[MutationOperatorsJSON]
  implicit val mutationOperatorsJSONEncoder: Encoder[MutationOperatorsJSON] = deriveEncoder[MutationOperatorsJSON]

  def generateMutationOperatorsJSONObjectFromMetrics(metrics: MutationOperatorsMetrics): MutationOperatorsJSON = {
    MutationOperatorsJSON(
      metrics.totalMutantsPerOperator,
      metrics.totalKilledMutantsPerOperator,
      metrics.totalLivedMutantsPerOperator,
      metrics.totalEquivalentMutantsPerOperator,
      metrics.totalErrorMutantsPerOperator,
      metrics.descriptionPerOperator)
  }

  def generateMutationOperatorsJSONFromMetrics(metrics: MutationOperatorsMetrics) = {
    generateMutationOperatorsJSONObjectFromMetrics(metrics).asJson
  }

  def mutationOperatorsJSONObjectFromJSONString(json: String): MutationOperatorsJSON = {
    decode[MutationOperatorsJSON](json).toOption.get
  }

  def mutationOperatorsJSONObjectFromJSON(json: Json): MutationOperatorsJSON = {
    json.as[MutationOperatorsJSON].toOption.get
  }
}