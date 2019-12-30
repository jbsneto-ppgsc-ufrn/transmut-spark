package br.ufrn.dimap.forall.transmut.model

object BaseTypesEnum extends Enumeration {

  type BaseTypesEnum = Value

  val Int, Long, Float, Double, Char, Boolean, String, Error = Value

  def baseTypeFromName(name: String) = name match {
    case n: String if Set("Int", "scala/Int#").contains(n) => BaseTypesEnum.Int
    case n: String if Set("Long", "scala/Long#").contains(n) => BaseTypesEnum.Long
    case n: String if Set("Float", "scala/Float#").contains(n) => BaseTypesEnum.Float
    case n: String if Set("Double", "scala/Double#").contains(n) => BaseTypesEnum.Double
    case n: String if Set("Char", "scala/Char#").contains(n) => BaseTypesEnum.Char
    case n: String if Set("Boolean", "scala/Boolean#").contains(n) => BaseTypesEnum.Boolean
    case n: String if Set("String", "scala/Predef.String#", "java/lang/String#").contains(n) => BaseTypesEnum.String
    case n => BaseTypesEnum.Error
  }

  def isBaseType(name: String) = baseTypsesStringSet.contains(name)
  
  def baseTypeName(bType: BaseTypesEnum.BaseTypesEnum) = bType match {
    case Int => "Int"
    case Long => "Long"
    case Float => "Float"
    case Double => "Double"
    case Char => "Char"
    case Boolean => "Boolean"
    case String => "String"
    case _ => "Error"
  }

  private val baseTypsesStringSet =
    Set(
      "Int",
      "Long",
      "Float",
      "Double",
      "Char",
      "Boolean",
      "String",
      "scala/Int#",
      "scala/Long#",
      "scala/Float#",
      "scala/Double#",
      "scala/Char#",
      "scala/Boolean#",
      "scala/Predef.String#",
      "java/lang/String#")

}
