package br.ufrn.dimap.forall.transmut.analyzer

import org.scalatest.FunSuite

import br.ufrn.dimap.forall.transmut.model.BaseType
import br.ufrn.dimap.forall.transmut.model.BaseTypesEnum
import br.ufrn.dimap.forall.transmut.model.ClassType
import br.ufrn.dimap.forall.transmut.model.ParameterizedType
import br.ufrn.dimap.forall.transmut.model.ReferencesTypeEnum
import br.ufrn.dimap.forall.transmut.model.TupleType

class TypesAnalyzerTestSuite extends FunSuite {
  
  test("Test Case 1 - Program with all base type values"){
   
    val refenceTypes = TypesAnalyzer.getReferenceMapFromPath("./src/test/resources/BaseTypesTestCase.scala.semanticdb") 
    
    assert(refenceTypes.contains("TypesAnalyzerBaseTypesTestCase"))
    assert(refenceTypes.get("TypesAnalyzerBaseTypesTestCase").get.valueType == ClassType("TypesAnalyzerBaseTypesTestCase"))
    
    assert(refenceTypes.contains("intValue"))
    assert(refenceTypes.get("intValue").get.valueType == BaseType(BaseTypesEnum.Int))
    
    assert(refenceTypes.contains("stringValue"))
    assert(refenceTypes.get("stringValue").get.valueType == BaseType(BaseTypesEnum.String))
    
    assert(refenceTypes.contains("floatValue"))
    assert(refenceTypes.get("floatValue").get.valueType == BaseType(BaseTypesEnum.Float))
    
    assert(refenceTypes.contains("doubleValue"))
    assert(refenceTypes.get("doubleValue").get.valueType == BaseType(BaseTypesEnum.Double))
    
    assert(refenceTypes.contains("longValue"))
    assert(refenceTypes.get("longValue").get.valueType == BaseType(BaseTypesEnum.Long))
    
    assert(refenceTypes.contains("charValue"))
    assert(refenceTypes.get("charValue").get.valueType == BaseType(BaseTypesEnum.Char))
    
    assert(refenceTypes.contains("booleanValue"))
    assert(refenceTypes.get("booleanValue").get.valueType == BaseType(BaseTypesEnum.Boolean))
  }
  
  test("Test Case 2 - Simple Spark Program"){
   
    val refenceTypes = TypesAnalyzer.getReferenceMapFromPath("./src/test/resources/SparkProgramTestCase1.scala.semanticdb") 
    
    assert(refenceTypes.contains("SparkProgramTestCase1"))
    assert(refenceTypes.get("SparkProgramTestCase1").get.valueType == ClassType("SparkProgramTestCase1"))
    
    assert(refenceTypes.contains("rdd1"))
    assert(refenceTypes.get("rdd1").get.referenceType == ReferencesTypeEnum.Parameter)
    assert(refenceTypes.get("rdd1").get.valueType.isInstanceOf[ParameterizedType])
    assert(refenceTypes.get("rdd1").get.valueType.asInstanceOf[ParameterizedType].name == "org/apache/spark/rdd/RDD#")
    assert(refenceTypes.get("rdd1").get.valueType.asInstanceOf[ParameterizedType].parametersTypes == List(BaseType(BaseTypesEnum.Int)))
    
    assert(refenceTypes.contains("rdd2"))
    assert(refenceTypes.get("rdd2").get.referenceType == ReferencesTypeEnum.Val)
    assert(refenceTypes.get("rdd2").get.valueType.isInstanceOf[ParameterizedType])
    assert(refenceTypes.get("rdd2").get.valueType.asInstanceOf[ParameterizedType].name == "org/apache/spark/rdd/RDD#")
    assert(refenceTypes.get("rdd2").get.valueType.asInstanceOf[ParameterizedType].parametersTypes == List(BaseType(BaseTypesEnum.String)))
    
    assert(refenceTypes.contains("rdd3"))
    assert(refenceTypes.get("rdd3").get.referenceType == ReferencesTypeEnum.Val)
    assert(refenceTypes.get("rdd3").get.valueType.isInstanceOf[ParameterizedType])
    assert(refenceTypes.get("rdd3").get.valueType.asInstanceOf[ParameterizedType].name == "org/apache/spark/rdd/RDD#")
    assert(refenceTypes.get("rdd3").get.valueType.asInstanceOf[ParameterizedType].parametersTypes == List(BaseType(BaseTypesEnum.String)))
    
    assert(refenceTypes.contains("rdd4"))
    assert(refenceTypes.get("rdd4").get.referenceType == ReferencesTypeEnum.Val)
    assert(refenceTypes.get("rdd4").get.valueType.isInstanceOf[ParameterizedType])
    assert(refenceTypes.get("rdd4").get.valueType.asInstanceOf[ParameterizedType].name == "org/apache/spark/rdd/RDD#")
    assert(refenceTypes.get("rdd4").get.valueType.asInstanceOf[ParameterizedType].parametersTypes == List(TupleType(BaseType(BaseTypesEnum.Int), BaseType(BaseTypesEnum.String))))
    
    assert(refenceTypes.contains("program"))
    assert(refenceTypes.get("program").get.referenceType == ReferencesTypeEnum.Method)
    assert(refenceTypes.get("program").get.valueType.isInstanceOf[ParameterizedType])
    assert(refenceTypes.get("program").get.valueType.asInstanceOf[ParameterizedType].name == "org/apache/spark/rdd/RDD#")
    assert(refenceTypes.get("program").get.valueType.asInstanceOf[ParameterizedType].parametersTypes == List(BaseType(BaseTypesEnum.Int)))
  }
  
}