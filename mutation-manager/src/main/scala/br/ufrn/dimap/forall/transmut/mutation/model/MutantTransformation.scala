package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.MutationOperatorsEnum

/**
 * A mutant tranformation (transformation with modifications into its name, source and parameters).
 * 
 * @constructor create a new mutant transformation with an original transformation, mutated transformation and mutation operator.
 * @param original transformation (transformation without modifications)
 * @param mutated transformation (transformation with modifications)
 * @param mutationOperator the mutation operator applied to generate this mutant.
 */
case class MutantTransformation(override val id : Long, val original : Transformation, val mutated : Transformation, val mutationOperator: MutationOperatorsEnum) extends Mutant[Transformation]