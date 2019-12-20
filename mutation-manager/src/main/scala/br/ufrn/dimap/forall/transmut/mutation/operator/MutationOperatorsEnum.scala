package br.ufrn.dimap.forall.transmut.mutation.operator

object MutationOperatorsEnum extends Enumeration {
  
  type MutationOperatorsEnum = Value
  
  val UTR = Value("Unary Transformation Replacement")
  val BTR = Value("Binary Transformation Replacement")
  val UTD = Value("Unary Transformation Deletion")
  val FTD = Value("Filter Transformation Deletion")
  val DTD = Value("Distinct Transformation Deletion")
  val OTD = Value("Order Transformation Deletion")
  val STR = Value("Set Transformation Replacement")
  val ATR = Value("Aggregation Transformation Replacement")
  val DTI = Value("Distinct Transformation Insertion")
  val JTR = Value("Join Transformation Replacement")
  
}