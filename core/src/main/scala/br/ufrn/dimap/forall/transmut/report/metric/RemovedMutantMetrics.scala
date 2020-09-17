package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.mutation.model.MutantListTransformation
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum

case class RemovedMutantMetrics(removedMutant: MutantRemoved) {

  def mutantId = removedMutant.mutant.id

  def originalProgramId = removedMutant.mutant.mutantProgram.original.id

  def originalProgramName = removedMutant.mutant.mutantProgram.original.name

  def originalProgramSourceId = removedMutant.mutant.original.id

  def originalProgramSourceName = removedMutant.mutant.original.source.getFileName.toString().replaceFirst(".scala", "")

  def name = removedMutant.mutant.mutantProgram.original.name

  def mutationOperator = removedMutant.mutant.mutationOperator

  def mutationOperatorName = MutationOperatorsEnum.mutationOperatorsNameFromEnum(mutationOperator)

  def mutationOperatorDescription = MutationOperatorsEnum.mutationOperatorsDescription(mutationOperator)
  
  def reductionRule = removedMutant.reductionRule
  
  def reductionRuleName = ReductionRulesEnum.reductionRulesNameFromEnum(reductionRule)

  def originalCode = removedMutant.mutant.mutantProgram.original.tree.syntax

  def listMutatedLinesOriginalProgram = {
    val originalTransformations = removedMutant.mutant.mutantProgram.mutantTransformations match {
      case m: MutantTransformation      => List(m.original)
      case ml: MutantListTransformation => ml.original
    }
    val originalCode = removedMutant.mutant.mutantProgram.original.tree.toString()
    val originalTransformationsCode = originalTransformations.map(o => o.source.toString())
    val lines = originalTransformationsCode.map(t => findLinesOfSentenceInText(originalCode, t))
    lines.reduce(_ ++ _)
  }

  def mutantCode = removedMutant.mutant.mutantProgram.mutated.tree.syntax

  def listMutatedLinesMutantProgram = {
    val mutantTransformations = removedMutant.mutant.mutantProgram.mutantTransformations match {
      case m: MutantTransformation      => List(m.mutated)
      case ml: MutantListTransformation => ml.mutated
    }
    val mutantCode = removedMutant.mutant.mutantProgram.mutated.tree.toString()
    val mutantTransformationsCode = mutantTransformations.map(o => o.source.toString())
    val lines = mutantTransformationsCode.map(t => findLinesOfSentenceInText(mutantCode, t))
    lines.reduce(_ ++ _)
  }
  
  private def findLinesOfSentenceInText(text: String, sentence: String): List[Int] = {
    val linesText = text.split("\n").zipWithIndex.map(a => ((a._2 + 1), a._1))
    val subSentences = sentence.split("\n").map(s => s.trim)
    subSentences.flatMap(s => linesText.filter(l => l._2.trim.contains(s)).map(sl => List(sl._1))).reduce(_ ++ _).toList
  }
}