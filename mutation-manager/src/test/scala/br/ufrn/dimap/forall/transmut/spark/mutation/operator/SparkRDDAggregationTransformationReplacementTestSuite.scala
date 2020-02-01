package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import scala.meta._
import scala.meta.Tree
import scala.meta.contrib._

import org.scalatest.FunSuite
import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDUnaryTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDBinaryTransformation
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDOperation
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

class SparkRDDAggregationTransformationReplacementTestSuite extends FunSuite {
  
  test("Test Case 1 - Not Applicable Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map(a => a + 1)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(!SparkRDDAggregationTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDAggregationTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.isEmpty)
  }

  test("Test Case 2 - reduceByKey with a lambda function") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[(Long, Double)]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.reduceByKey((x: Double, y: Double) => if(x > y) x else y)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val original = program.transformations.head

    assert(SparkRDDAggregationTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDAggregationTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 5)

    val mutantFirstParameter = mutants(0)
    val mutantSecondParameter = mutants(1)
    val mutantFirstParameterReplacement = mutants(2)
    val mutantSecondParameterReplacement = mutants(3)
    val mutantCommutativeReplacement = mutants(4)
    
    // Mutation Operator Type Verification
    assert(mutantFirstParameter.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantSecondParameter.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantFirstParameterReplacement.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantSecondParameterReplacement.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantCommutativeReplacement.mutationOperator == MutationOperatorsEnum.ATR)

    // First Parameter Mutant
    assert(mutantFirstParameter.original == original)
    assert(mutantFirstParameter.mutated != original)

    assert(mutantFirstParameter.mutated.id == mutantFirstParameter.original.id)
    assert(mutantFirstParameter.mutated.edges == mutantFirstParameter.original.edges)

    assert(mutantFirstParameter.mutated.name != mutantFirstParameter.original.name)
    assert(mutantFirstParameter.mutated.name == "firstParameter")

    assert(mutantFirstParameter.mutated.source != mutantFirstParameter.original.source)
    assert(mutantFirstParameter.original.source.isEqual(q"val rdd2 = rdd1.reduceByKey((x: Double, y: Double) => if(x > y) x else y)"))
    assert(mutantFirstParameter.mutated.source.isEqual(q"val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => firstParameter)"))

    assert(mutantFirstParameter.mutated.params.size == 1)
    assert(mutantFirstParameter.original.params.size == 1)
    assert(!mutantFirstParameter.mutated.params(0).isEqual(mutantFirstParameter.original.params(0)))
    assert(mutantFirstParameter.original.params(0).isEqual(q"(x: Double, y: Double) => if(x > y) x else y"))
    assert(mutantFirstParameter.mutated.params(0).isEqual(q"(firstParameter: Double, secondParameter: Double) => firstParameter"))

    // Second Parameter Mutant
    assert(mutantSecondParameter.original == original)
    assert(mutantSecondParameter.mutated != original)

    assert(mutantSecondParameter.mutated.id == mutantSecondParameter.original.id)
    assert(mutantSecondParameter.mutated.edges == mutantSecondParameter.original.edges)

    assert(mutantSecondParameter.mutated.name != mutantSecondParameter.original.name)
    assert(mutantSecondParameter.mutated.name == "secondParameter")

    assert(mutantSecondParameter.mutated.source != mutantSecondParameter.original.source)
    assert(mutantSecondParameter.original.source.isEqual(q"val rdd2 = rdd1.reduceByKey((x: Double, y: Double) => if(x > y) x else y)"))
    assert(mutantSecondParameter.mutated.source.isEqual(q"val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => secondParameter)"))

    assert(mutantSecondParameter.mutated.params.size == 1)
    assert(mutantSecondParameter.original.params.size == 1)
    assert(!mutantSecondParameter.mutated.params(0).isEqual(mutantSecondParameter.original.params(0)))
    assert(mutantSecondParameter.original.params(0).isEqual(q"(x: Double, y: Double) => if(x > y) x else y"))
    assert(mutantSecondParameter.mutated.params(0).isEqual(q"(firstParameter: Double, secondParameter: Double) => secondParameter"))

    // First Parameter Replacement Mutant
    assert(mutantFirstParameterReplacement.original == original)
    assert(mutantFirstParameterReplacement.mutated != original)

    assert(mutantFirstParameterReplacement.mutated.id == mutantFirstParameterReplacement.original.id)
    assert(mutantFirstParameterReplacement.mutated.edges == mutantFirstParameterReplacement.original.edges)

    assert(mutantFirstParameterReplacement.mutated.name != mutantFirstParameterReplacement.original.name)
    assert(mutantFirstParameterReplacement.mutated.name == "firstParameterReplacement")

    assert(mutantFirstParameterReplacement.mutated.source != mutantFirstParameterReplacement.original.source)
    assert(mutantFirstParameterReplacement.original.source.isEqual(q"val rdd2 = rdd1.reduceByKey((x: Double, y: Double) => if(x > y) x else y)"))
    assert(mutantFirstParameterReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => {
      val originalFunction = ((x: Double, y: Double) => if(x > y) x else y)(_,_)
      originalFunction(firstParameter, firstParameter)
      })"""))

    assert(mutantFirstParameterReplacement.mutated.params.size == 1)
    assert(mutantFirstParameterReplacement.original.params.size == 1)
    assert(!mutantFirstParameterReplacement.mutated.params(0).isEqual(mutantFirstParameterReplacement.original.params(0)))
    assert(mutantFirstParameterReplacement.original.params(0).isEqual(q"(x: Double, y: Double) => if(x > y) x else y"))
    assert(mutantFirstParameterReplacement.mutated.params(0).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
      val originalFunction = ((x: Double, y: Double) => if(x > y) x else y)(_,_)
      originalFunction(firstParameter, firstParameter)
      }"""))

    // Second Parameter Replacement Mutant
    assert(mutantSecondParameterReplacement.original == original)
    assert(mutantSecondParameterReplacement.mutated != original)

    assert(mutantSecondParameterReplacement.mutated.id == mutantSecondParameterReplacement.original.id)
    assert(mutantSecondParameterReplacement.mutated.edges == mutantSecondParameterReplacement.original.edges)

    assert(mutantSecondParameterReplacement.mutated.name != mutantSecondParameterReplacement.original.name)
    assert(mutantSecondParameterReplacement.mutated.name == "secondParameterReplacement")

    assert(mutantSecondParameterReplacement.mutated.source != mutantSecondParameterReplacement.original.source)
    assert(mutantSecondParameterReplacement.original.source.isEqual(q"val rdd2 = rdd1.reduceByKey((x: Double, y: Double) => if(x > y) x else y)"))
    assert(mutantSecondParameterReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => {
      val originalFunction = ((x: Double, y: Double) => if(x > y) x else y)(_,_)
      originalFunction(secondParameter, secondParameter)
      })"""))

    assert(mutantSecondParameterReplacement.mutated.params.size == 1)
    assert(mutantSecondParameterReplacement.original.params.size == 1)
    assert(!mutantSecondParameterReplacement.mutated.params(0).isEqual(mutantSecondParameterReplacement.original.params(0)))
    assert(mutantSecondParameterReplacement.original.params(0).isEqual(q"(x: Double, y: Double) => if(x > y) x else y"))
    assert(mutantSecondParameterReplacement.mutated.params(0).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
      val originalFunction = ((x: Double, y: Double) => if(x > y) x else y)(_,_)
      originalFunction(secondParameter, secondParameter)
      }"""))

    // Commutative Replacement Mutant
    assert(mutantCommutativeReplacement.original == original)
    assert(mutantCommutativeReplacement.mutated != original)

    assert(mutantCommutativeReplacement.mutated.id == mutantCommutativeReplacement.original.id)
    assert(mutantCommutativeReplacement.mutated.edges == mutantCommutativeReplacement.original.edges)

    assert(mutantCommutativeReplacement.mutated.name != mutantCommutativeReplacement.original.name)
    assert(mutantCommutativeReplacement.mutated.name == "commutativeReplacement")

    assert(mutantCommutativeReplacement.mutated.source != mutantCommutativeReplacement.original.source)
    assert(mutantCommutativeReplacement.original.source.isEqual(q"val rdd2 = rdd1.reduceByKey((x: Double, y: Double) => if(x > y) x else y)"))
    assert(mutantCommutativeReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => {
      val originalFunction = ((x: Double, y: Double) => if(x > y) x else y)(_,_)
      originalFunction(secondParameter, firstParameter)
      })"""))

    assert(mutantCommutativeReplacement.mutated.params.size == 1)
    assert(mutantCommutativeReplacement.original.params.size == 1)
    assert(!mutantCommutativeReplacement.mutated.params(0).isEqual(mutantCommutativeReplacement.original.params(0)))
    assert(mutantCommutativeReplacement.original.params(0).isEqual(q"(x: Double, y: Double) => if(x > y) x else y"))
    assert(mutantCommutativeReplacement.mutated.params(0).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
      val originalFunction = ((x: Double, y: Double) => if(x > y) x else y)(_,_)
      originalFunction(secondParameter, firstParameter)
      }"""))

  }

  test("Test Case 3 - reduceByKey with a reference function") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def max(x: Double, y: Double): Double = if(x > y) x else y
      
        def program(rdd1: RDD[(Long, Double)]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.reduceByKey(max)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val original = program.transformations.head

    assert(SparkRDDAggregationTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDAggregationTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 5)

    val mutantFirstParameter = mutants(0)
    val mutantSecondParameter = mutants(1)
    val mutantFirstParameterReplacement = mutants(2)
    val mutantSecondParameterReplacement = mutants(3)
    val mutantCommutativeReplacement = mutants(4)
    
    // Mutation Operator Type Verification
    assert(mutantFirstParameter.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantSecondParameter.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantFirstParameterReplacement.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantSecondParameterReplacement.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantCommutativeReplacement.mutationOperator == MutationOperatorsEnum.ATR)

    // First Parameter Mutant
    assert(mutantFirstParameter.original == original)
    assert(mutantFirstParameter.mutated != original)

    assert(mutantFirstParameter.mutated.id == mutantFirstParameter.original.id)
    assert(mutantFirstParameter.mutated.edges == mutantFirstParameter.original.edges)

    assert(mutantFirstParameter.mutated.name != mutantFirstParameter.original.name)
    assert(mutantFirstParameter.mutated.name == "firstParameter")

    assert(mutantFirstParameter.mutated.source != mutantFirstParameter.original.source)
    assert(mutantFirstParameter.original.source.isEqual(q"val rdd2 = rdd1.reduceByKey(max)"))
    assert(mutantFirstParameter.mutated.source.isEqual(q"val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => firstParameter)"))

    assert(mutantFirstParameter.mutated.params.size == 1)
    assert(mutantFirstParameter.original.params.size == 1)
    assert(!mutantFirstParameter.mutated.params(0).isEqual(mutantFirstParameter.original.params(0)))
    assert(mutantFirstParameter.original.params(0).isEqual(q"max"))
    assert(mutantFirstParameter.mutated.params(0).isEqual(q"(firstParameter: Double, secondParameter: Double) => firstParameter"))

    // Second Parameter Mutant
    assert(mutantSecondParameter.original == original)
    assert(mutantSecondParameter.mutated != original)

    assert(mutantSecondParameter.mutated.id == mutantSecondParameter.original.id)
    assert(mutantSecondParameter.mutated.edges == mutantSecondParameter.original.edges)

    assert(mutantSecondParameter.mutated.name != mutantSecondParameter.original.name)
    assert(mutantSecondParameter.mutated.name == "secondParameter")

    assert(mutantSecondParameter.mutated.source != mutantSecondParameter.original.source)
    assert(mutantSecondParameter.original.source.isEqual(q"val rdd2 = rdd1.reduceByKey(max)"))
    assert(mutantSecondParameter.mutated.source.isEqual(q"val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => secondParameter)"))

    assert(mutantSecondParameter.mutated.params.size == 1)
    assert(mutantSecondParameter.original.params.size == 1)
    assert(!mutantSecondParameter.mutated.params(0).isEqual(mutantSecondParameter.original.params(0)))
    assert(mutantSecondParameter.original.params(0).isEqual(q"max"))
    assert(mutantSecondParameter.mutated.params(0).isEqual(q"(firstParameter: Double, secondParameter: Double) => secondParameter"))

    // First Parameter Replacement Mutant
    assert(mutantFirstParameterReplacement.original == original)
    assert(mutantFirstParameterReplacement.mutated != original)

    assert(mutantFirstParameterReplacement.mutated.id == mutantFirstParameterReplacement.original.id)
    assert(mutantFirstParameterReplacement.mutated.edges == mutantFirstParameterReplacement.original.edges)

    assert(mutantFirstParameterReplacement.mutated.name != mutantFirstParameterReplacement.original.name)
    assert(mutantFirstParameterReplacement.mutated.name == "firstParameterReplacement")

    assert(mutantFirstParameterReplacement.mutated.source != mutantFirstParameterReplacement.original.source)
    assert(mutantFirstParameterReplacement.original.source.isEqual(q"val rdd2 = rdd1.reduceByKey(max)"))
    assert(mutantFirstParameterReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => {
      val originalFunction = (max)(_,_)
      originalFunction(firstParameter, firstParameter)
      })"""))

    assert(mutantFirstParameterReplacement.mutated.params.size == 1)
    assert(mutantFirstParameterReplacement.original.params.size == 1)
    assert(!mutantFirstParameterReplacement.mutated.params(0).isEqual(mutantFirstParameterReplacement.original.params(0)))
    assert(mutantFirstParameterReplacement.original.params(0).isEqual(q"max"))
    assert(mutantFirstParameterReplacement.mutated.params(0).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
      val originalFunction = (max)(_,_)
      originalFunction(firstParameter, firstParameter)
      }"""))

    // Second Parameter Replacement Mutant
    assert(mutantSecondParameterReplacement.original == original)
    assert(mutantSecondParameterReplacement.mutated != original)

    assert(mutantSecondParameterReplacement.mutated.id == mutantSecondParameterReplacement.original.id)
    assert(mutantSecondParameterReplacement.mutated.edges == mutantSecondParameterReplacement.original.edges)

    assert(mutantSecondParameterReplacement.mutated.name != mutantSecondParameterReplacement.original.name)
    assert(mutantSecondParameterReplacement.mutated.name == "secondParameterReplacement")

    assert(mutantSecondParameterReplacement.mutated.source != mutantSecondParameterReplacement.original.source)
    assert(mutantSecondParameterReplacement.original.source.isEqual(q"val rdd2 = rdd1.reduceByKey(max)"))
    assert(mutantSecondParameterReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => {
      val originalFunction = (max)(_,_)
      originalFunction(secondParameter, secondParameter)
      })"""))

    assert(mutantSecondParameterReplacement.mutated.params.size == 1)
    assert(mutantSecondParameterReplacement.original.params.size == 1)
    assert(!mutantSecondParameterReplacement.mutated.params(0).isEqual(mutantSecondParameterReplacement.original.params(0)))
    assert(mutantSecondParameterReplacement.original.params(0).isEqual(q"max"))
    assert(mutantSecondParameterReplacement.mutated.params(0).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
      val originalFunction = (max)(_,_)
      originalFunction(secondParameter, secondParameter)
      }"""))

    // Commutative Replacement Mutant
    assert(mutantCommutativeReplacement.original == original)
    assert(mutantCommutativeReplacement.mutated != original)

    assert(mutantCommutativeReplacement.mutated.id == mutantCommutativeReplacement.original.id)
    assert(mutantCommutativeReplacement.mutated.edges == mutantCommutativeReplacement.original.edges)

    assert(mutantCommutativeReplacement.mutated.name != mutantCommutativeReplacement.original.name)
    assert(mutantCommutativeReplacement.mutated.name == "commutativeReplacement")

    assert(mutantCommutativeReplacement.mutated.source != mutantCommutativeReplacement.original.source)
    assert(mutantCommutativeReplacement.original.source.isEqual(q"val rdd2 = rdd1.reduceByKey(max)"))
    assert(mutantCommutativeReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => {
      val originalFunction = (max)(_,_)
      originalFunction(secondParameter, firstParameter)
      })"""))

    assert(mutantCommutativeReplacement.mutated.params.size == 1)
    assert(mutantCommutativeReplacement.original.params.size == 1)
    assert(!mutantCommutativeReplacement.mutated.params(0).isEqual(mutantCommutativeReplacement.original.params(0)))
    assert(mutantCommutativeReplacement.original.params(0).isEqual(q"max"))
    assert(mutantCommutativeReplacement.mutated.params(0).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
      val originalFunction = (max)(_,_)
      originalFunction(secondParameter, firstParameter)
      }"""))

  }

  test("Test Case 4 - combineByKey with lambda functions") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[(Long, (Double, String))]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.String)))))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val original = program.transformations.head

    assert(SparkRDDAggregationTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDAggregationTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 5)

    val mutantFirstParameter = mutants(0)
    val mutantSecondParameter = mutants(1)
    val mutantFirstParameterReplacement = mutants(2)
    val mutantSecondParameterReplacement = mutants(3)
    val mutantCommutativeReplacement = mutants(4)
    
    // Mutation Operator Type Verification
    assert(mutantFirstParameter.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantSecondParameter.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantFirstParameterReplacement.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantSecondParameterReplacement.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantCommutativeReplacement.mutationOperator == MutationOperatorsEnum.ATR)

    // First Parameter Mutant
    assert(mutantFirstParameter.original == original)
    assert(mutantFirstParameter.mutated != original)

    assert(mutantFirstParameter.mutated.id == mutantFirstParameter.original.id)
    assert(mutantFirstParameter.mutated.edges == mutantFirstParameter.original.edges)

    assert(mutantFirstParameter.mutated.name != mutantFirstParameter.original.name)
    assert(mutantFirstParameter.mutated.name == "firstParameter")

    assert(mutantFirstParameter.mutated.source != mutantFirstParameter.original.source)
    assert(mutantFirstParameter.original.source.isEqual(q"""val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)"""))
    assert(mutantFirstParameter.mutated.source.isEqual(q"""val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (firstParameter: Double, secondParameter: Double) => firstParameter)"""))

    assert(mutantFirstParameter.mutated.params.size == 3)
    assert(mutantFirstParameter.original.params.size == 3)
    assert(mutantFirstParameter.mutated.params(0).isEqual(mutantFirstParameter.original.params(0)))
    assert(mutantFirstParameter.mutated.params(1).isEqual(mutantFirstParameter.original.params(1)))
    assert(mutantFirstParameter.original.params(2).isEqual(q"(accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2"))
    assert(mutantFirstParameter.mutated.params(2).isEqual(q"(firstParameter: Double, secondParameter: Double) => firstParameter"))

    // Second Parameter Mutant
    assert(mutantSecondParameter.original == original)
    assert(mutantSecondParameter.mutated != original)

    assert(mutantSecondParameter.mutated.id == mutantSecondParameter.original.id)
    assert(mutantSecondParameter.mutated.edges == mutantSecondParameter.original.edges)

    assert(mutantSecondParameter.mutated.name != mutantSecondParameter.original.name)
    assert(mutantSecondParameter.mutated.name == "secondParameter")

    assert(mutantSecondParameter.mutated.source != mutantSecondParameter.original.source)
    assert(mutantSecondParameter.original.source.isEqual(q"""val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)"""))
    assert(mutantSecondParameter.mutated.source.isEqual(q"""val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (firstParameter: Double, secondParameter: Double) => secondParameter)"""))

    assert(mutantSecondParameter.mutated.params.size == 3)
    assert(mutantSecondParameter.original.params.size == 3)
    assert(mutantSecondParameter.mutated.params(0).isEqual(mutantSecondParameter.original.params(0)))
    assert(mutantSecondParameter.mutated.params(1).isEqual(mutantSecondParameter.original.params(1)))
    assert(mutantSecondParameter.original.params(2).isEqual(q"(accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2"))
    assert(mutantSecondParameter.mutated.params(2).isEqual(q"(firstParameter: Double, secondParameter: Double) => secondParameter"))

    // First Parameter Replacement Mutant
    assert(mutantFirstParameterReplacement.original == original)
    assert(mutantFirstParameterReplacement.mutated != original)

    assert(mutantFirstParameterReplacement.mutated.id == mutantFirstParameterReplacement.original.id)
    assert(mutantFirstParameterReplacement.mutated.edges == mutantFirstParameterReplacement.original.edges)

    assert(mutantFirstParameterReplacement.mutated.name != mutantFirstParameterReplacement.original.name)
    assert(mutantFirstParameterReplacement.mutated.name == "firstParameterReplacement")

    assert(mutantFirstParameterReplacement.mutated.source != mutantFirstParameterReplacement.original.source)
    assert(mutantFirstParameterReplacement.original.source.isEqual(q"""val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)"""))
    assert(mutantFirstParameterReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (firstParameter: Double, secondParameter: Double) => {
              val originalFunction = ((accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)(_,_)
              originalFunction(firstParameter, firstParameter)
            })"""))

    assert(mutantFirstParameterReplacement.mutated.params.size == 3)
    assert(mutantFirstParameterReplacement.original.params.size == 3)
    assert(mutantFirstParameterReplacement.mutated.params(0).isEqual(mutantFirstParameterReplacement.original.params(0)))
    assert(mutantFirstParameterReplacement.mutated.params(1).isEqual(mutantFirstParameterReplacement.original.params(1)))
    assert(mutantFirstParameterReplacement.original.params(2).isEqual(q"(accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2"))
    assert(mutantFirstParameterReplacement.mutated.params(2).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
              val originalFunction = ((accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)(_,_)
              originalFunction(firstParameter, firstParameter)
            }"""))

    // Second Parameter Replacement Mutant
    assert(mutantSecondParameterReplacement.original == original)
    assert(mutantSecondParameterReplacement.mutated != original)

    assert(mutantSecondParameterReplacement.mutated.id == mutantSecondParameterReplacement.original.id)
    assert(mutantSecondParameterReplacement.mutated.edges == mutantSecondParameterReplacement.original.edges)

    assert(mutantSecondParameterReplacement.mutated.name != mutantSecondParameterReplacement.original.name)
    assert(mutantSecondParameterReplacement.mutated.name == "secondParameterReplacement")

    assert(mutantSecondParameterReplacement.mutated.source != mutantSecondParameterReplacement.original.source)
    assert(mutantSecondParameterReplacement.original.source.isEqual(q"""val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)"""))
    assert(mutantSecondParameterReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (firstParameter: Double, secondParameter: Double) => {
              val originalFunction = ((accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)(_,_)
              originalFunction(secondParameter, secondParameter)
            })"""))

    assert(mutantSecondParameterReplacement.mutated.params.size == 3)
    assert(mutantSecondParameterReplacement.original.params.size == 3)
    assert(mutantSecondParameterReplacement.mutated.params(0).isEqual(mutantSecondParameterReplacement.original.params(0)))
    assert(mutantSecondParameterReplacement.mutated.params(1).isEqual(mutantSecondParameterReplacement.original.params(1)))
    assert(mutantSecondParameterReplacement.original.params(2).isEqual(q"(accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2"))
    assert(mutantSecondParameterReplacement.mutated.params(2).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
              val originalFunction = ((accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)(_,_)
              originalFunction(secondParameter, secondParameter)
            }"""))

    // Commutative Replacement Mutant
    assert(mutantCommutativeReplacement.original == original)
    assert(mutantCommutativeReplacement.mutated != original)

    assert(mutantCommutativeReplacement.mutated.id == mutantSecondParameterReplacement.original.id)
    assert(mutantCommutativeReplacement.mutated.edges == mutantSecondParameterReplacement.original.edges)

    assert(mutantCommutativeReplacement.mutated.name != mutantSecondParameterReplacement.original.name)
    assert(mutantCommutativeReplacement.mutated.name == "commutativeReplacement")

    assert(mutantCommutativeReplacement.mutated.source != mutantCommutativeReplacement.original.source)
    assert(mutantCommutativeReplacement.original.source.isEqual(q"""val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)"""))
    assert(mutantCommutativeReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.combineByKey(
            (tuple: (Double, String)) => tuple._1,
            (accumulator: Double, element: (Double, String)) => accumulator + element._1,
            (firstParameter: Double, secondParameter: Double) => {
              val originalFunction = ((accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)(_,_)
              originalFunction(secondParameter, firstParameter)
            })"""))

    assert(mutantCommutativeReplacement.mutated.params.size == 3)
    assert(mutantCommutativeReplacement.original.params.size == 3)
    assert(mutantCommutativeReplacement.mutated.params(0).isEqual(mutantCommutativeReplacement.original.params(0)))
    assert(mutantCommutativeReplacement.mutated.params(1).isEqual(mutantCommutativeReplacement.original.params(1)))
    assert(mutantCommutativeReplacement.original.params(2).isEqual(q"(accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2"))
    assert(mutantCommutativeReplacement.mutated.params(2).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
              val originalFunction = ((accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2)(_,_)
              originalFunction(secondParameter, firstParameter)
            }"""))

  }

  test("Test Case 5 - combineByKey with reference functions") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def createCombiner = (tuple: (Double, String)) => tuple._1
        def mergeValue = (accumulator: Double, element: (Double, String)) => accumulator + element._1
        def mergeCombiner = (accumulator1: Double, accumulator2: Double) => accumulator1 + accumulator2
      
        def program(rdd1: RDD[(Long, (Double, String))]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.combineByKey(createCombiner, mergeValue, mergeCombiner)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.String)))))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val original = program.transformations.head

    assert(SparkRDDAggregationTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDAggregationTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.size == 5)

    val mutantFirstParameter = mutants(0)
    val mutantSecondParameter = mutants(1)
    val mutantFirstParameterReplacement = mutants(2)
    val mutantSecondParameterReplacement = mutants(3)
    val mutantCommutativeReplacement = mutants(4)
    
    // Mutation Operator Type Verification
    assert(mutantFirstParameter.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantSecondParameter.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantFirstParameterReplacement.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantSecondParameterReplacement.mutationOperator == MutationOperatorsEnum.ATR)
    assert(mutantCommutativeReplacement.mutationOperator == MutationOperatorsEnum.ATR)

    // First Parameter Mutant
    assert(mutantFirstParameter.original == original)
    assert(mutantFirstParameter.mutated != original)

    assert(mutantFirstParameter.mutated.id == mutantFirstParameter.original.id)
    assert(mutantFirstParameter.mutated.edges == mutantFirstParameter.original.edges)

    assert(mutantFirstParameter.mutated.name != mutantFirstParameter.original.name)
    assert(mutantFirstParameter.mutated.name == "firstParameter")

    assert(mutantFirstParameter.mutated.source != mutantFirstParameter.original.source)
    assert(mutantFirstParameter.original.source.isEqual(q"""val rdd2 = rdd1.combineByKey(createCombiner, mergeValue, mergeCombiner)"""))
    assert(mutantFirstParameter.mutated.source.isEqual(q"""val rdd2 = rdd1.combineByKey(createCombiner, mergeValue, 
      (firstParameter: Double, secondParameter: Double) => firstParameter)"""))

    assert(mutantFirstParameter.mutated.params.size == 3)
    assert(mutantFirstParameter.original.params.size == 3)
    assert(mutantFirstParameter.mutated.params(0).isEqual(mutantFirstParameter.original.params(0)))
    assert(mutantFirstParameter.mutated.params(1).isEqual(mutantFirstParameter.original.params(1)))
    assert(mutantFirstParameter.original.params(2).isEqual(q"mergeCombiner"))
    assert(mutantFirstParameter.mutated.params(2).isEqual(q"(firstParameter: Double, secondParameter: Double) => firstParameter"))

    // Second Parameter Mutant
    assert(mutantSecondParameter.original == original)
    assert(mutantSecondParameter.mutated != original)

    assert(mutantSecondParameter.mutated.id == mutantSecondParameter.original.id)
    assert(mutantSecondParameter.mutated.edges == mutantSecondParameter.original.edges)

    assert(mutantSecondParameter.mutated.name != mutantSecondParameter.original.name)
    assert(mutantSecondParameter.mutated.name == "secondParameter")

    assert(mutantSecondParameter.mutated.source != mutantSecondParameter.original.source)
    assert(mutantSecondParameter.original.source.isEqual(q"""val rdd2 = rdd1.combineByKey(createCombiner, mergeValue, mergeCombiner)"""))
    assert(mutantSecondParameter.mutated.source.isEqual(q"""val rdd2 = rdd1.combineByKey(createCombiner, mergeValue,
      (firstParameter: Double, secondParameter: Double) => secondParameter)"""))

    assert(mutantSecondParameter.mutated.params.size == 3)
    assert(mutantSecondParameter.original.params.size == 3)
    assert(mutantSecondParameter.mutated.params(0).isEqual(mutantSecondParameter.original.params(0)))
    assert(mutantSecondParameter.mutated.params(1).isEqual(mutantSecondParameter.original.params(1)))
    assert(mutantSecondParameter.original.params(2).isEqual(q"mergeCombiner"))
    assert(mutantSecondParameter.mutated.params(2).isEqual(q"(firstParameter: Double, secondParameter: Double) => secondParameter"))

    // First Parameter Replacement Mutant
    assert(mutantFirstParameterReplacement.original == original)
    assert(mutantFirstParameterReplacement.mutated != original)

    assert(mutantFirstParameterReplacement.mutated.id == mutantFirstParameterReplacement.original.id)
    assert(mutantFirstParameterReplacement.mutated.edges == mutantFirstParameterReplacement.original.edges)

    assert(mutantFirstParameterReplacement.mutated.name != mutantFirstParameterReplacement.original.name)
    assert(mutantFirstParameterReplacement.mutated.name == "firstParameterReplacement")

    assert(mutantFirstParameterReplacement.mutated.source != mutantFirstParameterReplacement.original.source)
    assert(mutantFirstParameterReplacement.original.source.isEqual(q"""val rdd2 = rdd1.combineByKey(createCombiner, mergeValue, mergeCombiner)"""))
    assert(mutantFirstParameterReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.combineByKey(createCombiner, mergeValue,
      (firstParameter: Double, secondParameter: Double) => {
              val originalFunction = (mergeCombiner)(_,_)
              originalFunction(firstParameter, firstParameter)
            })"""))

    assert(mutantFirstParameterReplacement.mutated.params.size == 3)
    assert(mutantFirstParameterReplacement.original.params.size == 3)
    assert(mutantFirstParameterReplacement.mutated.params(0).isEqual(mutantFirstParameterReplacement.original.params(0)))
    assert(mutantFirstParameterReplacement.mutated.params(1).isEqual(mutantFirstParameterReplacement.original.params(1)))
    assert(mutantFirstParameterReplacement.original.params(2).isEqual(q"mergeCombiner"))
    assert(mutantFirstParameterReplacement.mutated.params(2).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
              val originalFunction = (mergeCombiner)(_,_)
              originalFunction(firstParameter, firstParameter)
            }"""))

    // Second Parameter Replacement Mutant
    assert(mutantSecondParameterReplacement.original == original)
    assert(mutantSecondParameterReplacement.mutated != original)

    assert(mutantSecondParameterReplacement.mutated.id == mutantSecondParameterReplacement.original.id)
    assert(mutantSecondParameterReplacement.mutated.edges == mutantSecondParameterReplacement.original.edges)

    assert(mutantSecondParameterReplacement.mutated.name != mutantSecondParameterReplacement.original.name)
    assert(mutantSecondParameterReplacement.mutated.name == "secondParameterReplacement")

    assert(mutantSecondParameterReplacement.mutated.source != mutantSecondParameterReplacement.original.source)
    assert(mutantSecondParameterReplacement.original.source.isEqual(q"""val rdd2 = rdd1.combineByKey(createCombiner, mergeValue, mergeCombiner)"""))
    assert(mutantSecondParameterReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.combineByKey(createCombiner, mergeValue,
      (firstParameter: Double, secondParameter: Double) => {
              val originalFunction = (mergeCombiner)(_,_)
              originalFunction(secondParameter, secondParameter)
            })"""))

    assert(mutantSecondParameterReplacement.mutated.params.size == 3)
    assert(mutantSecondParameterReplacement.original.params.size == 3)
    assert(mutantSecondParameterReplacement.mutated.params(0).isEqual(mutantSecondParameterReplacement.original.params(0)))
    assert(mutantSecondParameterReplacement.mutated.params(1).isEqual(mutantSecondParameterReplacement.original.params(1)))
    assert(mutantSecondParameterReplacement.original.params(2).isEqual(q"mergeCombiner"))
    assert(mutantSecondParameterReplacement.mutated.params(2).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
              val originalFunction = (mergeCombiner)(_,_)
              originalFunction(secondParameter, secondParameter)
            }"""))

    // Commutative Replacement Mutant
    assert(mutantCommutativeReplacement.original == original)
    assert(mutantCommutativeReplacement.mutated != original)

    assert(mutantCommutativeReplacement.mutated.id == mutantSecondParameterReplacement.original.id)
    assert(mutantCommutativeReplacement.mutated.edges == mutantSecondParameterReplacement.original.edges)

    assert(mutantCommutativeReplacement.mutated.name != mutantSecondParameterReplacement.original.name)
    assert(mutantCommutativeReplacement.mutated.name == "commutativeReplacement")

    assert(mutantCommutativeReplacement.mutated.source != mutantCommutativeReplacement.original.source)
    assert(mutantCommutativeReplacement.original.source.isEqual(q"""val rdd2 = rdd1.combineByKey(createCombiner, mergeValue, mergeCombiner)"""))
    assert(mutantCommutativeReplacement.mutated.source.isEqual(q"""val rdd2 = rdd1.combineByKey(createCombiner, mergeValue,
      (firstParameter: Double, secondParameter: Double) => {
              val originalFunction = (mergeCombiner)(_,_)
              originalFunction(secondParameter, firstParameter)
            })"""))

    assert(mutantCommutativeReplacement.mutated.params.size == 3)
    assert(mutantCommutativeReplacement.original.params.size == 3)
    assert(mutantCommutativeReplacement.mutated.params(0).isEqual(mutantCommutativeReplacement.original.params(0)))
    assert(mutantCommutativeReplacement.mutated.params(1).isEqual(mutantCommutativeReplacement.original.params(1)))
    assert(mutantCommutativeReplacement.original.params(2).isEqual(q"mergeCombiner"))
    assert(mutantCommutativeReplacement.mutated.params(2).isEqual(q"""(firstParameter: Double, secondParameter: Double) => {
              val originalFunction = (mergeCombiner)(_,_)
              originalFunction(secondParameter, firstParameter)
            }"""))

  }
}