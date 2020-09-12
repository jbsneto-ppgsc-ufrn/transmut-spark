package br.ufrn.dimap.forall.transmut.spark.mutation.reduction

import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.mutation.model.MutantTransformation
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRule
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum._
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved

/**
 * Reduction rule that removes MTR mutants that are easily killed or that are equivalent in most cases.
 *
 * Mutants that are removed according to the type of mapping:
 *
 * Numbers: MaxValue and MinValue
 *
 * String: ""
 *
 * Collections: reverse
 *
 * General: null
 */
object SparkRDDMTRReductionRule extends ReductionRule {

  val mutantsToRemove = Set("MaxValue", "MinValue", "\"\"", "reverse", "null")

  def reductionRuleType: ReductionRulesEnum = MTRR

  def isReducible(mutants: List[MutantProgramSource]): Boolean = {
    mutants.exists(m => m.mutationOperator == MTR && mutantsToRemove.exists(p => m.mutantProgram.mutantTransformations.asInstanceOf[MutantTransformation].mutated.name.contains(p)))
  }

  def reduceMutants(mutants: List[MutantProgramSource]): (List[MutantProgramSource], List[MutantRemoved]) = {
    if (isReducible(mutants)) {
      val newMutants = scala.collection.mutable.ListBuffer.empty[MutantProgramSource]
      val removedMutants = scala.collection.mutable.ListBuffer.empty[MutantRemoved]
      mutants.foreach { m =>
        if (m.mutationOperator == MTR) {
          val mutated = m.mutantProgram.mutantTransformations.asInstanceOf[MutantTransformation].mutated
          if (mutantsToRemove.exists(p => mutated.name.contains(p))) {
            removedMutants += MutantRemoved(m, reductionRuleType)
          } else {
            newMutants += m
          }
        } else {
          newMutants += m
        }
      }
      (newMutants.toList, removedMutants.toList)
    } else {
      (mutants, Nil)
    }
  }

}