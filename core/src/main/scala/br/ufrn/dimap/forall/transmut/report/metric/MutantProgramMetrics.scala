package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgram
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantKilled
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantError
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantSurvived
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantEquivalent
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.mutation.model.MutantListTransformation
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation

case class MutantProgramMetrics(mutant: MutantProgram, mutantVerdict: MutantResult[MutantProgram]) {

  def mutantId = mutant.id

  def originalProgramId = mutant.original.id

  def originalProgramName = mutant.original.name

  def originalProgramSourceId = mutant.original.programSource.id

  def originalProgramSourceName = mutant.original.programSource.source.getFileName.toString().replaceFirst(".scala", "")

  def name = mutant.original.name

  def mutationOperator = mutant.mutationOperator

  def mutationOperatorName = MutationOperatorsEnum.mutationOperatorsNameFromEnum(mutant.mutationOperator)

  def mutationOperatorDescription = MutationOperatorsEnum.mutationOperatorsDescription(mutant.mutationOperator)

  def originalCode = mutant.original.tree.syntax

  def listMutatedLinesOriginalProgram = {
    val originalTransformations = mutant.mutantTransformations match {
      case m: MutantTransformation      => List(m.original)
      case ml: MutantListTransformation => ml.original
    }
    val originalCode = mutant.original.tree.toString()
    val originalTransformationsCode = originalTransformations.map(o => o.source.toString())
    val lines = originalTransformationsCode.map(t => findLinesOfSentenceInText(originalCode, t))
    lines.reduce(_ ++ _)
  }

  def mutantCode = mutant.mutated.tree.syntax

  def listMutatedLinesMutantProgram = {
    val mutantTransformations = mutant.mutantTransformations match {
      case m: MutantTransformation      => List(m.mutated)
      case ml: MutantListTransformation => ml.mutated
    }
    val mutantCode = mutant.mutated.tree.toString()
    val mutantTransformationsCode = mutantTransformations.map(o => o.source.toString())
    val lines = mutantTransformationsCode.map(t => findLinesOfSentenceInText(mutantCode, t))
    lines.reduce(_ ++ _)
  }

  def status = mutantVerdict match {
    case MutantSurvived(m)   => "Survived"
    case MutantEquivalent(m) => "Equivalent"
    case MutantKilled(m)     => "Killed"
    case _                   => "Error"
  }

  private def findLinesOfSentenceInText(text: String, sentence: String): List[Int] = {
    val linesText = text.split("\n").zipWithIndex.map(a => ((a._2 + 1), a._1))
    val subSentences = sentence.split("\n").map(s => s.trim)
    subSentences.flatMap(s => linesText.filter(l => l._2.trim.contains(s)).map(sl => List(sl._1))).reduce(_ ++ _).toList
  }

}