package br.ufrn.dimap.forall.transmut.mutation.operator

object MutationOperatorsEnum extends Enumeration {
  
  type MutationOperatorsEnum = Value
  
  val FTD = Value("Filter Transformation Deletion")
  val DTD = Value("Distinct Transformation Deletion")
  val OTD = Value("Order Transformation Deletion")
  val STR = Value("Set Transformation Replacement")
  
}