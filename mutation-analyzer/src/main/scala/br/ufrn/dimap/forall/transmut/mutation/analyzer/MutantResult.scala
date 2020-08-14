package br.ufrn.dimap.forall.transmut.mutation.analyzer

import br.ufrn.dimap.forall.transmut.mutation.model.Mutant

abstract class MutantResult[T](val mutant: T) {
  require(mutant.isInstanceOf[Mutant[_]])
}
case class MutantKilled[T](override val mutant: T) extends MutantResult(mutant)
case class MutantLived[T](override val mutant: T) extends MutantResult(mutant)
case class MutantEquivalent[T](override val mutant: T) extends MutantResult(mutant)
case class MutantError[T](override val mutant: T) extends MutantResult(mutant)