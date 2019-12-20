package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._

case class MutantListTransformation(override val id : Long, val original : List[Transformation], val mutated : List[Transformation], val mutationOperator: MutationOperatorsEnum) extends Mutant[List[Transformation]] 