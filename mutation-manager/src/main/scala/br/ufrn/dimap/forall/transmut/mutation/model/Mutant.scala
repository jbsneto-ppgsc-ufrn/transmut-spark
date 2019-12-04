package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.Element

trait Mutant[T] extends Element {
  def original : T
  def mutated : T
}