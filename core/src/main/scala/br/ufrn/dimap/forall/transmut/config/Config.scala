package br.ufrn.dimap.forall.transmut.config

import java.nio.file._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.util.DateTimeUtil
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum

case class Config(
  sources:             List[String],
  programs:            List[String],
  mutationOperators:   List[String] = List("ALL"),
  equivalentMutants:   List[Long]   = List(),
  enableReduction:     Boolean      = true,
  reductionRules:      List[String] = List("ALL"),
  var forceExecution:  List[Long]   = List(),
  var testOnly:        List[String] = List(),
  var srcDir:          Path         = Paths.get("src/main/scala/"),
  var semanticdbDir:   Path         = Paths.get("target/scala-2.12/meta/"),
  var transmutBaseDir: Path         = Paths.get("target/")) {

  val defaultSrcDir: Path = Paths.get("src/main/scala/")
  val defaultSemanticdbDir: Path = Paths.get("target/scala-2.12/meta/")
  val defaultTransmutBaseDir: Path = Paths.get("target/")

  private var _processStartDateTime = LocalDateTime.now()

  def processStartDateTime = _processStartDateTime

  def processStartDateTime_=(dateTime: LocalDateTime) = {
    _processStartDateTime = dateTime
  }

  def updateProcessStartTimeToNow = {
    processStartDateTime = DateTimeUtil.getCurrentDateTime
  }

  val transmutDirName = "transmut-" + DateTimeUtil.getDatestampFromDateTime(processStartDateTime)
  val transmutDir = Paths.get(transmutBaseDir.toString(), transmutDirName)
  val transmutReportsDir = Paths.get(transmutDir.toString(), "reports")
  val transmutHtmlReportsDir = Paths.get(transmutReportsDir.toString(), "html")
  val transmutJSONReportsDir = Paths.get(transmutReportsDir.toString(), "json")
  val transmutSrcDir = Paths.get(transmutDir.toString(), "mutated-src")
  val transmutMutantsDir = Paths.get(transmutDir.toString(), "mutants")

  var livingMutants: List[Long] = List()
  var testLivingMutants: Boolean = false

  def isTestOnlyLivingMutants = testLivingMutants

  def isTestOnlyEnabled = !testOnly.isEmpty
  def testOnlyParameters = " " + testOnly.mkString(" ")

  var killedForceExecutionMutants: List[Long] = List()
  def remainingForceExecution: List[Long] = forceExecution.filter(id => !killedForceExecutionMutants.contains(id))
  var enableForceExecution = false
  def isForceExecutionEnabled = enableForceExecution && !forceExecution.isEmpty

  def mutationOperatorsList: List[MutationOperatorsEnum.MutationOperatorsEnum] = {
    if (mutationOperators.contains("ALL"))
      MutationOperatorsEnum.ALL
    else
      mutationOperators.map(MutationOperatorsEnum.mutationOperatorsEnumFromName)
  }

  def reductionRulesList: List[ReductionRulesEnum.ReductionRulesEnum] = {
    if (reductionRules.contains("ALL")) {
      ReductionRulesEnum.ALL
    } else {
      reductionRules.map(ReductionRulesEnum.reductionRulesEnumFromName)
    }
  }

}