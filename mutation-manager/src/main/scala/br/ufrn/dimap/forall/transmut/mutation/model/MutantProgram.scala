package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.Program
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.MutationOperatorsEnum

/**
 * A mutant Program that have a mutant transformation or a list of mutant transformations.
 * A mutant program only exists with a mutant transformation or list of transformations, so its id and mutarionOperator are the same as the mutant transformation or list of transformations.
 *
 * @constructor create a new mutant program with an original program, mutated program and a mutant transformation.
 * @param original program
 * @param mutated program
 * @param mutantTransformations the mutant transformation or list of transformations
 */
case class MutantProgram(override val original: Program, override val mutated: Program, mutantTransformations: Mutant[_]) extends Mutant[Program] {

  // Pre-condition: the mutant program must have a mutant transformation or a mutant list of transformations (dataflow mutant)
  assert(mutantTransformations.isInstanceOf[MutantTransformation] || mutantTransformations.isInstanceOf[MutantListTransformation])

  override val id: Long = mutantTransformations.id

  override val mutationOperator: MutationOperatorsEnum = mutantTransformations.mutationOperator

}