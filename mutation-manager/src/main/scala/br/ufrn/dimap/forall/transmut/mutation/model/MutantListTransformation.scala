package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.MutationOperatorsEnum

/**
 * A mutant list of tranformations (a list of transformations with modifications into their names, sources and parameters).
 * Represents a dataflow mutant.
 * 
 * @constructor create a new mutant list of transformations with an original list of transformations, mutated list of transformations and mutation operator.
 * @param original list of transformations (transformations without modifications)
 * @param mutated list of transformations (transformations with modifications)
 * @param mutationOperator the mutation operator applied to generate this mutant.
 */
case class MutantListTransformation(override val id : Long, val original : List[Transformation], val mutated : List[Transformation], val mutationOperator: MutationOperatorsEnum) extends Mutant[List[Transformation]]