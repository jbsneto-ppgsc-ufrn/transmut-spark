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

class SparkRDDJoinTransformationReplacementTestSuite extends FunSuite {
  
  test("Test Case 1 - Join") {

    val idGenerator = LongIdGenerator.generator
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, String)]) : RDD[(Long, (Double, String))] = {
          val rdd3 = rdd1.join(rdd2)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.String))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.String)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val original = program.transformations.head

    assert(SparkRDDJoinTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDJoinTransformationReplacement.generateMutants(original, idGenerator)
    
    assert(mutants.size == 3)
    
    val mutantLeftOuterJoin = mutants(0)
    val mutantRightOuterJoin = mutants(1)
    val mutantFullOuterJoin = mutants(2)
    
    // Mutation Operator Type Verification
    assert(mutantLeftOuterJoin.mutationOperator == MutationOperatorsEnum.JTR)
    assert(mutantRightOuterJoin.mutationOperator == MutationOperatorsEnum.JTR)
    assert(mutantFullOuterJoin.mutationOperator == MutationOperatorsEnum.JTR)
    
    // leftOuterJoin Mutant
    assert(mutantLeftOuterJoin.original == original)
    assert(mutantLeftOuterJoin.mutated != original)

    assert(mutantLeftOuterJoin.mutated.id == mutantLeftOuterJoin.original.id)
    assert(mutantLeftOuterJoin.mutated.edges == mutantLeftOuterJoin.original.edges)

    assert(mutantLeftOuterJoin.mutated.name != mutantLeftOuterJoin.original.name)
    assert(mutantLeftOuterJoin.mutated.name == "leftOuterJoin")

    assert(mutantLeftOuterJoin.mutated.source != mutantLeftOuterJoin.original.source)
    assert(mutantLeftOuterJoin.original.source.isEqual(q"val rdd3 = rdd1.join(rdd2)"))
    assert(mutantLeftOuterJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.leftOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1, tuple._2._2.getOrElse(""))))"""))

    assert(mutantLeftOuterJoin.mutated.params == mutantLeftOuterJoin.original.params)

    // rightOuterJoin Mutant
    assert(mutantRightOuterJoin.original == original)
    assert(mutantRightOuterJoin.mutated != original)

    assert(mutantRightOuterJoin.mutated.id == mutantRightOuterJoin.original.id)
    assert(mutantRightOuterJoin.mutated.edges == mutantRightOuterJoin.original.edges)

    assert(mutantRightOuterJoin.mutated.name != mutantRightOuterJoin.original.name)
    assert(mutantRightOuterJoin.mutated.name == "rightOuterJoin")

    assert(mutantRightOuterJoin.mutated.source != mutantRightOuterJoin.original.source)
    assert(mutantRightOuterJoin.original.source.isEqual(q"val rdd3 = rdd1.join(rdd2)"))
    assert(mutantRightOuterJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.rightOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1.getOrElse(0d), tuple._2._2)))"""))

    assert(mutantRightOuterJoin.mutated.params == mutantRightOuterJoin.original.params)
    
    // fullOuterJoin Mutant
    assert(mutantFullOuterJoin.original == original)
    assert(mutantFullOuterJoin.mutated != original)

    assert(mutantFullOuterJoin.mutated.id == mutantFullOuterJoin.original.id)
    assert(mutantFullOuterJoin.mutated.edges == mutantFullOuterJoin.original.edges)

    assert(mutantFullOuterJoin.mutated.name != mutantFullOuterJoin.original.name)
    assert(mutantFullOuterJoin.mutated.name == "fullOuterJoin")

    assert(mutantFullOuterJoin.mutated.source != mutantFullOuterJoin.original.source)
    assert(mutantFullOuterJoin.original.source.isEqual(q"val rdd3 = rdd1.join(rdd2)"))
    assert(mutantFullOuterJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.fullOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1.getOrElse(0d), tuple._2._2.getOrElse(""))))"""))

    assert(mutantFullOuterJoin.mutated.params == mutantFullOuterJoin.original.params)

  }
  
  test("Test Case 2 - LeftOuterJoin") {

    val idGenerator = LongIdGenerator.generator
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[(Long, Int)], rdd2: RDD[(Long, Char)]) : RDD[(Long, (Int, Option[Char]))] = {
          val rdd3 = rdd1.leftOuterJoin(rdd2)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Int))))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Char))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Int), ParameterizedType("Option", List(BaseType(BaseTypesEnum.Char)))))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val original = program.transformations.head

    assert(SparkRDDJoinTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDJoinTransformationReplacement.generateMutants(original, idGenerator)
    
    assert(mutants.size == 3)
    
    val mutantJoin = mutants(0)
    val mutantRightOuterJoin = mutants(1)
    val mutantFullOuterJoin = mutants(2)
    
    // Mutation Operator Type Verification
    assert(mutantJoin.mutationOperator == MutationOperatorsEnum.JTR)
    assert(mutantRightOuterJoin.mutationOperator == MutationOperatorsEnum.JTR)
    assert(mutantFullOuterJoin.mutationOperator == MutationOperatorsEnum.JTR)
    
    // Join Mutant
    assert(mutantJoin.original == original)
    assert(mutantJoin.mutated != original)

    assert(mutantJoin.mutated.id == mutantJoin.original.id)
    assert(mutantJoin.mutated.edges == mutantJoin.original.edges)

    assert(mutantJoin.mutated.name != mutantJoin.original.name)
    assert(mutantJoin.mutated.name == "join")

    assert(mutantJoin.mutated.source != mutantJoin.original.source)
    assert(mutantJoin.original.source.isEqual(q"val rdd3 = rdd1.leftOuterJoin(rdd2)"))
    assert(mutantJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.join(rdd2).map(tuple => (tuple._1, (tuple._2._1, Option(tuple._2._2))))"""))

    assert(mutantJoin.mutated.params == mutantJoin.original.params)

    // rightOuterJoin Mutant
    assert(mutantRightOuterJoin.original == original)
    assert(mutantRightOuterJoin.mutated != original)

    assert(mutantRightOuterJoin.mutated.id == mutantRightOuterJoin.original.id)
    assert(mutantRightOuterJoin.mutated.edges == mutantRightOuterJoin.original.edges)

    assert(mutantRightOuterJoin.mutated.name != mutantRightOuterJoin.original.name)
    assert(mutantRightOuterJoin.mutated.name == "rightOuterJoin")

    assert(mutantRightOuterJoin.mutated.source != mutantRightOuterJoin.original.source)
    assert(mutantRightOuterJoin.original.source.isEqual(q"val rdd3 = rdd1.leftOuterJoin(rdd2)"))
    assert(mutantRightOuterJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.rightOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1.getOrElse(0), Option(tuple._2._2))))"""))

    assert(mutantRightOuterJoin.mutated.params == mutantRightOuterJoin.original.params)
    
    // fullOuterJoin Mutant
    assert(mutantFullOuterJoin.original == original)
    assert(mutantFullOuterJoin.mutated != original)

    assert(mutantFullOuterJoin.mutated.id == mutantFullOuterJoin.original.id)
    assert(mutantFullOuterJoin.mutated.edges == mutantFullOuterJoin.original.edges)

    assert(mutantFullOuterJoin.mutated.name != mutantFullOuterJoin.original.name)
    assert(mutantFullOuterJoin.mutated.name == "fullOuterJoin")

    assert(mutantFullOuterJoin.mutated.source != mutantFullOuterJoin.original.source)
    assert(mutantFullOuterJoin.original.source.isEqual(q"val rdd3 = rdd1.leftOuterJoin(rdd2)"))
    assert(mutantFullOuterJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.fullOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1.getOrElse(0), tuple._2._2)))"""))

    assert(mutantFullOuterJoin.mutated.params == mutantFullOuterJoin.original.params)

  }
  
  test("Test Case 3 - RightOuterJoin") {

    val idGenerator = LongIdGenerator.generator
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[(Long, Char)], rdd2: RDD[(Long, Boolean)]) : RDD[(Long, (Option[Char], Boolean))] = {
          val rdd3 = rdd1.rightOuterJoin(rdd2)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Char))))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Boolean))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(ParameterizedType("Option", List(BaseType(BaseTypesEnum.Char))), BaseType(BaseTypesEnum.Boolean)))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val original = program.transformations.head

    assert(SparkRDDJoinTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDJoinTransformationReplacement.generateMutants(original, idGenerator)
    
    assert(mutants.size == 3)
    
    val mutantJoin = mutants(0)
    val mutantLeftOuterJoin = mutants(1)
    val mutantFullOuterJoin = mutants(2)
    
    // Mutation Operator Type Verification
    assert(mutantJoin.mutationOperator == MutationOperatorsEnum.JTR)
    assert(mutantLeftOuterJoin.mutationOperator == MutationOperatorsEnum.JTR)
    assert(mutantFullOuterJoin.mutationOperator == MutationOperatorsEnum.JTR)
    
    // Join Mutant
    assert(mutantJoin.original == original)
    assert(mutantJoin.mutated != original)

    assert(mutantJoin.mutated.id == mutantJoin.original.id)
    assert(mutantJoin.mutated.edges == mutantJoin.original.edges)

    assert(mutantJoin.mutated.name != mutantJoin.original.name)
    assert(mutantJoin.mutated.name == "join")

    assert(mutantJoin.mutated.source != mutantJoin.original.source)
    assert(mutantJoin.original.source.isEqual(q"val rdd3 = rdd1.rightOuterJoin(rdd2)"))
    assert(mutantJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.join(rdd2).map(tuple => (tuple._1, (Option(tuple._2._1), tuple._2._2)))"""))

    assert(mutantJoin.mutated.params == mutantJoin.original.params)

    // rightOuterJoin Mutant
    assert(mutantLeftOuterJoin.original == original)
    assert(mutantLeftOuterJoin.mutated != original)

    assert(mutantLeftOuterJoin.mutated.id == mutantLeftOuterJoin.original.id)
    assert(mutantLeftOuterJoin.mutated.edges == mutantLeftOuterJoin.original.edges)

    assert(mutantLeftOuterJoin.mutated.name != mutantLeftOuterJoin.original.name)
    assert(mutantLeftOuterJoin.mutated.name == "leftOuterJoin")

    assert(mutantLeftOuterJoin.mutated.source != mutantLeftOuterJoin.original.source)
    assert(mutantLeftOuterJoin.original.source.isEqual(q"val rdd3 = rdd1.rightOuterJoin(rdd2)"))
    assert(mutantLeftOuterJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.leftOuterJoin(rdd2).map(tuple => (tuple._1, (Option(tuple._2._1), tuple._2._2.getOrElse(false))))"""))

    assert(mutantLeftOuterJoin.mutated.params == mutantLeftOuterJoin.original.params)
    
    // fullOuterJoin Mutant
    assert(mutantFullOuterJoin.original == original)
    assert(mutantFullOuterJoin.mutated != original)

    assert(mutantFullOuterJoin.mutated.id == mutantFullOuterJoin.original.id)
    assert(mutantFullOuterJoin.mutated.edges == mutantFullOuterJoin.original.edges)

    assert(mutantFullOuterJoin.mutated.name != mutantFullOuterJoin.original.name)
    assert(mutantFullOuterJoin.mutated.name == "fullOuterJoin")

    assert(mutantFullOuterJoin.mutated.source != mutantFullOuterJoin.original.source)
    assert(mutantFullOuterJoin.original.source.isEqual(q"val rdd3 = rdd1.rightOuterJoin(rdd2)"))
    assert(mutantFullOuterJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.fullOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1, tuple._2._2.getOrElse(false))))"""))

    assert(mutantFullOuterJoin.mutated.params == mutantFullOuterJoin.original.params)

  }
  
  test("Test Case 4 - FullOuterJoin") {

    val idGenerator = LongIdGenerator.generator
    
    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[(Long, Char)], rdd2: RDD[(Long, Boolean)]) : RDD[(Long, (Option[Char], Option[Boolean]))] = {
          val rdd3 = rdd1.fullOuterJoin(rdd2)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Char))))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Boolean))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(ParameterizedType("Option", List(BaseType(BaseTypesEnum.Char))), ParameterizedType("Option", List(BaseType(BaseTypesEnum.Boolean)))))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val original = program.transformations.head

    assert(SparkRDDJoinTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDJoinTransformationReplacement.generateMutants(original, idGenerator)
    
    assert(mutants.size == 3)
    
    val mutantJoin = mutants(0)
    val mutantLeftOuterJoin = mutants(1)
    val mutantRightOuterJoin = mutants(2)
    
    // Mutation Operator Type Verification
    assert(mutantJoin.mutationOperator == MutationOperatorsEnum.JTR)
    assert(mutantLeftOuterJoin.mutationOperator == MutationOperatorsEnum.JTR)
    assert(mutantRightOuterJoin.mutationOperator == MutationOperatorsEnum.JTR)
    
    // Join Mutant
    assert(mutantJoin.original == original)
    assert(mutantJoin.mutated != original)

    assert(mutantJoin.mutated.id == mutantJoin.original.id)
    assert(mutantJoin.mutated.edges == mutantJoin.original.edges)

    assert(mutantJoin.mutated.name != mutantJoin.original.name)
    assert(mutantJoin.mutated.name == "join")

    assert(mutantJoin.mutated.source != mutantJoin.original.source)
    assert(mutantJoin.original.source.isEqual(q"val rdd3 = rdd1.fullOuterJoin(rdd2)"))
    assert(mutantJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.join(rdd2).map(tuple => (tuple._1, (Option(tuple._2._1), Option(tuple._2._2))))"""))

    assert(mutantJoin.mutated.params == mutantJoin.original.params)

    // rightOuterJoin Mutant
    assert(mutantLeftOuterJoin.original == original)
    assert(mutantLeftOuterJoin.mutated != original)

    assert(mutantLeftOuterJoin.mutated.id == mutantLeftOuterJoin.original.id)
    assert(mutantLeftOuterJoin.mutated.edges == mutantLeftOuterJoin.original.edges)

    assert(mutantLeftOuterJoin.mutated.name != mutantLeftOuterJoin.original.name)
    assert(mutantLeftOuterJoin.mutated.name == "leftOuterJoin")

    assert(mutantLeftOuterJoin.mutated.source != mutantLeftOuterJoin.original.source)
    assert(mutantLeftOuterJoin.original.source.isEqual(q"val rdd3 = rdd1.fullOuterJoin(rdd2)"))
    assert(mutantLeftOuterJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.leftOuterJoin(rdd2).map(tuple => (tuple._1, (Option(tuple._2._1), tuple._2._2)))"""))

    assert(mutantLeftOuterJoin.mutated.params == mutantLeftOuterJoin.original.params)
    
    // fullOuterJoin Mutant
    assert(mutantRightOuterJoin.original == original)
    assert(mutantRightOuterJoin.mutated != original)

    assert(mutantRightOuterJoin.mutated.id == mutantRightOuterJoin.original.id)
    assert(mutantRightOuterJoin.mutated.edges == mutantRightOuterJoin.original.edges)

    assert(mutantRightOuterJoin.mutated.name != mutantRightOuterJoin.original.name)
    assert(mutantRightOuterJoin.mutated.name == "rightOuterJoin")

    assert(mutantRightOuterJoin.mutated.source != mutantRightOuterJoin.original.source)
    assert(mutantRightOuterJoin.original.source.isEqual(q"val rdd3 = rdd1.fullOuterJoin(rdd2)"))
    assert(mutantRightOuterJoin.mutated.source.isEqual(q"""val rdd3 = rdd1.rightOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1, Option(tuple._2._2))))"""))

    assert(mutantRightOuterJoin.mutated.params == mutantRightOuterJoin.original.params)

  }
  
  test("Test Case 5 - Not Applicable Unary Transformation") {

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

    assert(!SparkRDDJoinTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDJoinTransformationReplacement.generateMutants(original, idGenerator)

    assert(mutants.isEmpty)
  }
  
  test("Test Case 6 - Not Applicable Binary Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.SparkContext
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.union(rdd2)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    val original = program.transformations.head

    assert(!SparkRDDJoinTransformationReplacement.isApplicable(original))

    val mutants = SparkRDDJoinTransformationReplacement.generateMutants(original, idGenerator)
    
    assert(mutants.isEmpty)
    
  }
  
}