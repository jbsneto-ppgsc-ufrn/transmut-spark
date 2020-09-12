package br.ufrn.dimap.forall.transmut.spark.mutation.manager

import br.ufrn.dimap.forall.transmut.mutation.manager.MutantReducer
import br.ufrn.dimap.forall.transmut.mutation.reduction.ReductionRulesEnum._
import br.ufrn.dimap.forall.transmut.mutation.reduction.MutantRemoved
import br.ufrn.dimap.forall.transmut.mutation.model.MutantProgramSource
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDProgramSource
import br.ufrn.dimap.forall.transmut.spark.mutation.reduction._

object SparkRDDMutantReducer extends MutantReducer {

  def reduceMutantsList(mutants: List[MutantProgramSource], reductionRules: List[ReductionRulesEnum]): (List[MutantProgramSource], List[MutantRemoved]) = {
    if (mutants.forall(m => m.mutated.isInstanceOf[SparkRDDProgramSource])) {
      var newMutants = mutants
      var removedMutants = List[MutantRemoved]()
      for (rule <- reductionRules) {
        rule match {
          case UTDE => {
            val (newMut, removed) = SparkRDDUTDEqualReductionRule.reduceMutants(newMutants)
            newMutants = newMut
            removedMutants ++= removed
          }
          case FTDS => {
            val (newMut, removed) = SparkRDDFTDSubsumptionReductionRule.reduceMutants(newMutants)
            newMutants = newMut
            removedMutants ++= removed
          }
          case OTDS => {
            val (newMut, removed) = SparkRDDOTDSubsumptionReductionRule.reduceMutants(newMutants)
            newMutants = newMut
            removedMutants ++= removed
          }
          case DTIE => {
            val (newMut, removed) = SparkRDDDTIEquivalentReductionRule.reduceMutants(newMutants)
            newMutants = newMut
            removedMutants ++= removed
          }
          case ATRC => {
            val (newMut, removed) = SparkRDDATRCommutativeReductionRule.reduceMutants(newMutants)
            newMutants = newMut
            removedMutants ++= removed  
          }
          case MTRR => {
            val (newMut, removed) = SparkRDDMTRReductionRule.reduceMutants(newMutants)
            newMutants = newMut
            removedMutants ++= removed
          }
        }
      }
      (newMutants, removedMutants)
    } else {
      (mutants, Nil)
    }
  }

}