package br.ufrn.dimap.forall.transmut.config

import java.nio.file._
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

case class Config(
  sources:            List[String],
  programs:           List[String],
  mutationOperators:  List[String] = List("ALL"),
  equivalentMutants:  List[Long]   = List(),
  var testOnly: List[String] = List(),
  var srcDir:         Path         = Paths.get("src/main/scala/"),
  var semanticdbDir:  Path         = Paths.get("target/scala-2.12/meta/"),
  var transmutDir:    Path         = Paths.get("target/transmut/")) {

  val defaultSrcDir: Path = Paths.get("src/main/scala/")
  val defaultSemanticdbDir: Path = Paths.get("target/scala-2.12/meta/")
  val defaultTransmutDir: Path = Paths.get("target/transmut/")
  
  val transmutReportsDir = Paths.get(transmutDir.toString(), "reports")
  val transmutHtmlReportsDir = Paths.get(transmutReportsDir.toString(), "html")
  val transmutJSONReportsDir = Paths.get(transmutReportsDir.toString(), "json")
  val transmutSrcDir = Paths.get(transmutDir.toString(), "mutated-src")
  val transmutMutantsDir = Paths.get(transmutDir.toString(), "mutants")
  
  def isTestOnlyEnabled = !testOnly.isEmpty
  def testOnlyParameters = " " + testOnly.mkString(" ")

  def mutationOperatorsList: List[MutationOperatorsEnum.MutationOperatorsEnum] = {
    if (mutationOperators.contains("ALL"))
      MutationOperatorsEnum.ALL
    else
      mutationOperators.map(MutationOperatorsEnum.mutationOperatorsEnumFromName)
  }

}