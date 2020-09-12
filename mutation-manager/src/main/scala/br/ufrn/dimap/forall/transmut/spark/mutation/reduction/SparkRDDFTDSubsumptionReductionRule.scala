package br.ufrn.dimap.forall.transmut.spark.mutation.reduction

import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.FTD
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.NFTP
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum.UTD
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRule
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum.FTDS
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum.ReductionRulesEnum

/**
 * Reduction rule that removes NFTP mutants if there are FTD or UTD (which also includes FTD) mutants.
 */
object SparkRDDFTDSubsumptionReductionRule extends ReductionRule {
  
  def reductionRuleType: ReductionRulesEnum = FTDS

  def isReducible(mutants: List[MutantProgramSource]): Boolean = {
    mutants.exists(m => m.mutationOperator == UTD || m.mutationOperator == FTD) && mutants.exists(m => m.mutationOperator == NFTP)
  }

  def reduceMutants(mutants: List[MutantProgramSource]): (List[MutantProgramSource], List[MutantRemoved]) = {
    if (isReducible(mutants)) {
      val newMutants = scala.collection.mutable.ListBuffer.empty[MutantProgramSource]
      val removedMutants = scala.collection.mutable.ListBuffer.empty[MutantRemoved]
      mutants.foreach { m =>
        if (m.mutationOperator != NFTP) {
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