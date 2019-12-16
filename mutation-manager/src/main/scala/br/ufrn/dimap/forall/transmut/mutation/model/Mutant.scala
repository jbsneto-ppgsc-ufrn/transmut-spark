package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.Element
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._

trait Mutant[T] extends Element {
  def original : T
  def mutated : T
  def mutationOperator: MutationOperatorsEnum
}