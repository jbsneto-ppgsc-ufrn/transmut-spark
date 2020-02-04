package br.ufrn.dimap.forall.transmut.mutation.operator

object MutationOperatorsEnum extends Enumeration {
  
  type MutationOperatorsEnum = Value
  
  val UTS = Value("Unary Transformation Swap")
  val BTS = Value("Binary Transformation Swap")
  val UTR = Value("Unary Transformation Replacement")
  val BTR = Value("Binary Transformation Replacement")
  val UTD = Value("Unary Transformation Deletion")
  val MTR = Value("Mapping Transformation Replacement")
  val FTD = Value("Filter Transformation Deletion")
  val STR = Value("Set Transformation Replacement")
  val DTD = Value("Distinct Transformation Deletion")
  val DTI = Value("Distinct Transformation Insertion")
  val ATR = Value("Aggregation Transformation Replacement")
  val JTR = Value("Join Transformation Replacement")
  val OTD = Value("Order Transformation Deletion")
  
  def ALL: List[MutationOperatorsEnum] = List(UTS, BTS, UTR, BTR, UTD, MTR, FTD, STR, DTD, DTI, ATR, JTR, OTD)
  
  def mutationOperatorsEnumFromName(operatorName: String): MutationOperatorsEnum = operatorName match {
    case "UTS" => UTS
    case "BTS" => BTS
    case "UTR" => UTR
    case "BTR" => BTR
    case "UTD" => UTD
    case "MTR" => MTR
    case "FTD" => FTD
    case "DTD" => DTD
    case "OTD" => OTD
    case "STR" => STR
    case "ATR" => ATR
    case "DTI" => DTI
    case "JTR" => JTR
    case _ => throw new Exception("Inexistent Mutation Operator")
  }
  
  def mutationOperatorsNameFromEnum(operator: MutationOperatorsEnum): String = operator match {
    case UTS => "UTS"
    case BTS => "BTS"
    case UTR => "UTR"
    case BTR => "BTR"
    case UTD => "UTD"
    case MTR => "MTR"
    case FTD => "FTD"
    case DTD => "DTD"
    case OTD => "OTD"
    case STR => "STR"
    case ATR => "ATR"
    case DTI => "DTI"
    case JTR => "JTR"
    case _ => throw new Exception("Inexistent Mutation Operator")
  }
  
  def mutationOperatorsDescription(operator: MutationOperatorsEnum): String = operator match {
    case UTS => "Unary Transformation Swap"
    case BTS => "Binary Transformation Swap"
    case UTR => "Unary Transformation Replacement"
    case BTR => "Binary Transformation Replacement"
    case UTD => "Unary Transformation Deletion"
    case MTR => "Mapping Transformation Replacement"
    case FTD => "Filter Transformation Deletion"
    case DTD => "Distinct Transformation Deletion"
    case OTD => "Order Transformation Deletion"
    case STR => "Set Transformation Replacement"
    case ATR => "Aggregation Transformation Replacement"
    case DTI => "Distinct Transformation Insertion"
    case JTR => "Join Transformation Replacement"
    case _ => throw new Exception("Inexistent Mutation Operator")
  }
  
}