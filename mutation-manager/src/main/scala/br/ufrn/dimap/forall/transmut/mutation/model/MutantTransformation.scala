package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.model.Edge
import scala.meta.Tree

case class MutantTransformation(override val id : Long, val original : Transformation, val mutated : Transformation) extends Mutant[Transformation] 