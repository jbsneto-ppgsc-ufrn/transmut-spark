package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.Element

trait MetaMutant[T] extends Element {
  def original : T
  def mutated : T
  def mutants : List[Mutant[T]]
}