package br.ufrn.dimap.forall.transmut.model

import ReferencesTypeEnum._

abstract class Reference(val name : String, val referenceType: ReferencesTypeEnum, val valueType : Type)

case class ParameterReference(override val name : String, override val valueType : Type) extends Reference(name, ReferencesTypeEnum.Parameter, valueType)

case class ValReference(override val name : String, override val valueType : Type) extends Reference(name, ReferencesTypeEnum.Val, valueType)

case class VarReference(override val name : String, override val valueType : Type) extends Reference(name, ReferencesTypeEnum.Var, valueType)

case class MethodReference(override val name : String, returnType : Type) extends Reference(name, ReferencesTypeEnum.Method, returnType)

case class ClassReference(override val name : String) extends Reference(name, ReferencesTypeEnum.Class, ClassType(name))

case class TypeReference(override val name : String) extends Reference(name, ReferencesTypeEnum.Type, ClassType(name))

case class ErrorReference() extends Reference("error", ReferencesTypeEnum.Error, ErrorType())