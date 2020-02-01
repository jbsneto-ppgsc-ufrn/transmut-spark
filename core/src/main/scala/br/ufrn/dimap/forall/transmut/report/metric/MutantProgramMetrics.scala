package br.ufrn.dimap.forall.transmut.report.metric

import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgram
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantResult
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantKilled
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantError
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantSurvived
import br.ufrn.dimap.forall.transmut.mutation.analyzer.MutantEquivalent
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

case class MutantProgramMetrics(mutant: MutantProgram, mutantVerdict: MutantResult[MutantProgram]) {

  def mutantId = mutant.id

  def originalProgramId = mutant.id

  def name = mutant.original.name
  
  def mutationOperator = MutationOperatorsEnum.mutationOperatorsNameFromEnum(mutant.mutationOperator)

  def originalCode = mutant.original.tree.syntax

  def mutantCode = mutant.mutated.tree.syntax

  def status = mutantVerdict match {
    case MutantSurvived(m)   => "Survived"
    case MutantEquivalent(m) => "Equivalent"
    case MutantKilled(m)     => "Killed"
    case _                   => "Error"
  }

}