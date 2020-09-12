package br.ufrn.dimap.forall.transmut.mutation.reduction

object ReductionRulesEnum extends Enumeration {

  type ReductionRulesEnum = Value

  val UTDE = Value("UTD Equal")
  val FTDS = Value("FTD Subsumption")
  val OTDS = Value("OTD Subsumption")
  val DTIE = Value("DTI Equivalent")
  val ATRC = Value("ATR Commutative")
  val MTRR = Value("MTR Reduction")

  def ALL: List[ReductionRulesEnum] = List(UTDE, FTDS, OTDS, DTIE, ATRC, MTRR)

  def reductionRulesEnumFromName(reductionRule: String): ReductionRulesEnum = reductionRule match {
    case "UTDE" => UTDE
    case "FTDS" => FTDS
    case "OTDS" => OTDS
    case "DTIE" => DTIE
    case "ATRC" => ATRC
    case "MTRR" => MTRR
    case _      => throw new Exception("Inexistent Reduction Rule")
  }

  def reductionRulesNameFromEnum(reductionRule: ReductionRulesEnum): String = reductionRule match {
    case UTDE => "UTDE"
    case FTDS => "FTDS"
    case OTDS => "OTDS"
    case DTIE => "DTIE"
    case ATRC => "ATRC"
    case MTRR => "MTRR"
    case _    => throw new Exception("Inexistent Reduction Rule")
  }

}