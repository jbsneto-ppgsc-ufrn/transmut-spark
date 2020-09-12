package br.ufrn.dimap.forall.transmut.spark.mutation.reduction

import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.OTD
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.OTI
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.UTD
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRule
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum.OTDS
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum.ReductionRulesEnum

/**
 * Reduction rule that removes OTI mutants if there are OTD or UTD (which also includes OTD) mutants.
 */
object SparkRDDOTDSubsumptionReductionRule extends ReductionRule {

  def reductionRuleType: ReductionRulesEnum = OTDS
  
  def isReducible(mutants: List[MutantProgramSource]): Boolean = {
    mutants.exists(m => m.mutationOperator == UTD || m.mutationOperator == OTD) && mutants.exists(m => m.mutationOperator == OTI)
  }

  def reduceMutants(mutants: List[MutantProgramSource]): (List[MutantProgramSource], List[MutantRemoved]) = {
    if (isReducible(mutants)) {
      val newMutants = scala.collection.mutable.ListBuffer.empty[MutantProgramSource]
      val removedMutants = scala.collection.mutable.ListBuffer.empty[MutantRemoved]
      mutants.foreach { m =>
        if (m.mutationOperator != OTI) {
          newMutants += m
        } else {
          removedMutants += MutantRemoved(m, reductionRuleType)
        }
      }
      (newMutants.toList, removedMutants.toList)
    } else {
      (mutants, Nil)
    }
  }

}