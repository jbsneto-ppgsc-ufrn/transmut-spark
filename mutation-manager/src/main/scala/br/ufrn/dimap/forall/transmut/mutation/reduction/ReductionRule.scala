package br.ufrn.dimap.forall.transmut.mutation.reduction

import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum._

trait ReductionRule {

  /**
   * Reduction rule type.
   */
  def reductionRuleType: ReductionRulesEnum

  /**
   * Method that checks whether the list of mutants can be reduced or not with this reduction rule.
   *
   * @param mutants list
   * @return a Boolean value indicating whether the list of mutants can be reduced
   */
  def isReducible(mutants: List[MutantProgramSource]): Boolean

  /**
   * Method that applies the reduction rule and returns a tuple containing the new list of mutants (without the removed mutants) as the first element and the list of removed mutants as the second element.
   *
   * @param mutants list
   * @return a tuple where the first element is the new list of mutants (without the removed mutants) and the second element is the list of removed mutants
   */
  def reduceMutants(mutants: List[MutantProgramSource]): (List[MutantProgramSource], List[MutantRemoved])

}