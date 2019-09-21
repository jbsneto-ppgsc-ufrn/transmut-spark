package br.ufrn.dimap.forall.model

object ReferencesTypeEnum extends Enumeration {
  type ReferencesTypeEnum = Value
  val Class, Method, Type, Val, Var, Def, Parameter, Error = Value
}