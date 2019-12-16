package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.model.Edge
import scala.meta.Tree
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._

case class MutantTransformation(override val id : Long, val original : Transformation, val mutated : Transformation, val mutationOperator: MutationOperatorsEnum) extends Mutant[Transformation] 