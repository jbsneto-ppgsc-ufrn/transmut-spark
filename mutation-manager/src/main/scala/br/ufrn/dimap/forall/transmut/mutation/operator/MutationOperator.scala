package br.ufrn.dimap.forall.transmut.mutation.operator

import br.ufrn.dimap.forall.transmut.mutation.model.Mutant

import MutationOperatorsEnum._
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator

trait MutationOperator[T] {
  
  def mutationOperatorType : MutationOperatorsEnum
  
  def isApplicable(element: T) : Boolean
  
  def generateMutants(element : T, idGenerator : LongIdGenerator) : List[Mutant[T]]
  
}