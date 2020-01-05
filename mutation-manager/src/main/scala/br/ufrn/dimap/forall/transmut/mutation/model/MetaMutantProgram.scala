package br.ufrn.dimap.forall.transmut.mutation.model

import br.ufrn.dimap.forall.transmut.model.Program

case class MetaMutantProgram (override val original: Program, override val mutated: Program, override val mutants: List[MutantProgram]) extends MetaMutant[Program] {
  override val id: Long = original.id
}