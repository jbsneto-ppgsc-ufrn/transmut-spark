package br.ufrn.dimap.forall.transmut.mutation.reduction

import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum.ReductionRulesEnum

case class MutantRemoved(val mutant: MutantProgramSource, val reductionRule: ReductionRulesEnum)