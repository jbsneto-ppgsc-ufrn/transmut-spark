package br.ufrn.dimap.forall.transmut.model

import BaseTypesEnum._

abstract class Type(val name: String) {

  override def toString = simplifiedName

  def simplifiedName = name.replace('/', '.').split('.').last.replace("#", "")

}

case class BaseType(tType: BaseTypesEnum) extends Type(tType.toString())

case class ClassType(override val name: String) extends Type(name)

case class TupleType(key: Type, value: Type) extends Type("Tuple2") {

  override def simplifiedName = "(" + key.simplifiedName + ", " + value.simplifiedName + ")"

}

case class ParameterizedType(override val name: String, parametersTypes: List[Type]) extends Type(name) {

  override def simplifiedName = super.simplifiedName + "[" + parametersTypes.map(p => p.simplifiedName).mkString(", ") + "]"

}

case class ErrorType() extends Type("Error")
