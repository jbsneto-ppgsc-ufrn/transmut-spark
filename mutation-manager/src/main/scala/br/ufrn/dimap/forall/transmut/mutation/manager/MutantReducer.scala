package br.ufrn.dimap.forall.transmut.mutation.manager

import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum._

trait MutantReducer {

  /**
   * Method that manages the reduction of the list of mutants to execute in the mutation testing process.
   * It applies reduction rules to the list of mutants passed as a parameter and returns both the new list of mutants and the list of removed mutants.
   *
   * @param mutants list
   * @return a tuple where the first element is the new list of mutants (without the removed mutants) and the second element is the list of removed mutants
   */
  def reduceMutantsList(mutants: List[MutantProgramSource], reductionRules: List[ReductionRulesEnum]): (List[MutantProgramSource], List[MutantRemoved])

}