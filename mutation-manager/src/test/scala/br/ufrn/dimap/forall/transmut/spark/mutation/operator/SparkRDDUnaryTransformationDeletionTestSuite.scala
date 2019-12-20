package br.ufrn.dimap.forall.transmut.spark.mutation.operator

import org.scalatest.FunSuite
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.model.Reference
import scala.meta._
import scala.meta.contrib._
import br.ufrn.dimap.forall.transmut.model.BaseTypesEnum
import br.ufrn.dimap.forall.transmut.model.ValReference
import br.ufrn.dimap.forall.transmut.model.ParameterReference
import br.ufrn.dimap.forall.transmut.model.BaseType
import br.ufrn.dimap.forall.transmut.model.ParameterizedType
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum
import br.ufrn.dimap.forall.transmut.model.TupleType

class SparkRDDUnaryTransformationDeletionTestSuite extends FunSuite {

  test("Test Case 1 - Two Applicable Transformations") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map(a => a + 1)
          val rdd3 = rdd2.filter(a => a < 100)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val originals = programSource.programs.head.transformations

    assert(SparkRDDUnaryTransformationDeletion.isApplicable(originals))

    val mutants = SparkRDDUnaryTransformationDeletion.generateMutants(originals, idGenerator)

    assert(mutants.size == 2)

    val mutant1 = mutants(0)

    assert(mutant1.mutationOperator == MutationOperatorsEnum.UTD)

    assert(mutant1.original.size == 1)
    assert(originals.contains(mutant1.original.head))
    assert(mutant1.mutated.size == 1)
    assert(!originals.contains(mutant1.mutated.head))

    assert(mutant1.mutated.head.id == mutant1.original.head.id)
    assert(mutant1.mutated.head.edges == mutant1.original.head.edges)

    assert(mutant1.mutated.head.name != mutant1.original.head.name)
    assert(mutant1.mutated.head.name == "mapDeletion")

    assert(mutant1.mutated.head.source != mutant1.original.head.source)
    assert(mutant1.original.head.source.isEqual(q"val rdd2 = rdd1.map(a => a + 1)"))
    assert(mutant1.mutated.head.source.isEqual(q"val rdd2 = rdd1"))

    assert(mutant1.mutated.head.params != mutant1.original.head.params)
    assert(mutant1.mutated.head.params.isEmpty)

    val mutant2 = mutants(1)

    assert(mutant2.mutationOperator == MutationOperatorsEnum.UTD)

    assert(mutant2.original.size == 1)
    assert(originals.contains(mutant2.original.head))
    assert(mutant2.mutated.size == 1)
    assert(!originals.contains(mutant2.mutated.head))

    assert(mutant2.mutated.head.id == mutant2.original.head.id)
    assert(mutant2.mutated.head.edges == mutant2.original.head.edges)

    assert(mutant2.mutated.head.name != mutant2.original.head.name)
    assert(mutant2.mutated.head.name == "filterDeletion")

    assert(mutant2.mutated.head.source != mutant2.original.head.source)
    assert(mutant2.original.head.source.isEqual(q"val rdd3 = rdd2.filter(a => a < 100)"))
    assert(mutant2.mutated.head.source.isEqual(q"val rdd3 = rdd2"))

    assert(mutant2.mutated.head.params != mutant2.original.head.params)
    assert(mutant2.mutated.head.params.isEmpty)

  }

  test("Test Case 2 - One Applicable Transformation Without Parameters and One Not Applicable Binary Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.union(rdd2)
          val rdd4 = rdd3.distinct
          rdd4
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val originals = programSource.programs.head.transformations

    assert(SparkRDDUnaryTransformationDeletion.isApplicable(originals))

    val mutants = SparkRDDUnaryTransformationDeletion.generateMutants(originals, idGenerator)

    assert(mutants.size == 1)

    val mutant1 = mutants(0)

    assert(mutant1.mutationOperator == MutationOperatorsEnum.UTD)

    assert(mutant1.original.size == 1)
    assert(originals.contains(mutant1.original.head))
    assert(mutant1.mutated.size == 1)
    assert(!originals.contains(mutant1.mutated.head))

    assert(mutant1.mutated.head.id == mutant1.original.head.id)
    assert(mutant1.mutated.head.edges == mutant1.original.head.edges)

    assert(mutant1.mutated.head.name != mutant1.original.head.name)
    assert(mutant1.mutated.head.name == "distinctDeletion")

    assert(mutant1.mutated.head.source != mutant1.original.head.source)
    assert(mutant1.original.head.source.isEqual(q"val rdd4 = rdd3.distinct"))
    assert(mutant1.mutated.head.source.isEqual(q"val rdd4 = rdd3"))

    assert(mutant1.mutated.head.params == mutant1.original.head.params)
    assert(mutant1.mutated.head.params.isEmpty)

  }

  test("Test Case 3 - Two Applicable Transformations and One Not Applicable Unary Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.map(x => x.toInt)
          val rdd3 = rdd2.filter(x => x % 2 == 0)
          val rdd4 = rdd3.distinct()
          rdd4
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val originals = programSource.programs.head.transformations

    assert(SparkRDDUnaryTransformationDeletion.isApplicable(originals))

    val mutants = SparkRDDUnaryTransformationDeletion.generateMutants(originals, idGenerator)

    assert(mutants.size == 2)

    val mutant1 = mutants(0)
    val mutant2 = mutants(1)

    assert(mutant1.mutationOperator == MutationOperatorsEnum.UTD)

    assert(mutant1.original.size == 1)
    assert(originals.contains(mutant1.original.head))
    assert(mutant1.mutated.size == 1)
    assert(!originals.contains(mutant1.mutated.head))

    assert(mutant1.mutated.head.id == mutant1.original.head.id)
    assert(mutant1.mutated.head.edges == mutant1.original.head.edges)

    assert(mutant1.mutated.head.name != mutant1.original.head.name)
    assert(mutant1.mutated.head.name == "filterDeletion")

    assert(mutant1.mutated.head.source != mutant1.original.head.source)
    assert(mutant1.original.head.source.isEqual(q"val rdd3 = rdd2.filter(x => x % 2 == 0)"))
    assert(mutant1.mutated.head.source.isEqual(q"val rdd3 = rdd2"))

    assert(mutant1.mutated.head.params != mutant1.original.head.params)
    assert(mutant1.mutated.head.params.isEmpty)

    assert(mutant2.mutationOperator == MutationOperatorsEnum.UTD)

    assert(mutant2.original.size == 1)
    assert(originals.contains(mutant2.original.head))
    assert(mutant2.mutated.size == 1)
    assert(!originals.contains(mutant2.mutated.head))

    assert(mutant2.mutated.head.id == mutant2.original.head.id)
    assert(mutant2.mutated.head.edges == mutant2.original.head.edges)

    assert(mutant2.mutated.head.name != mutant2.original.head.name)
    assert(mutant2.mutated.head.name == "distinctDeletion")

    assert(mutant2.mutated.head.source != mutant2.original.head.source)
    assert(mutant2.original.head.source.isEqual(q"val rdd4 = rdd3.distinct()"))
    assert(mutant2.mutated.head.source.isEqual(q"val rdd4 = rdd3"))

    assert(mutant2.mutated.head.params == mutant2.original.head.params)
    assert(mutant2.mutated.head.params.isEmpty)

  }

  test("Test Case 4 - One Not Applicable Unary Transformations and One Applicable Aggregation Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) : RDD[(String, Int)] = {
          val rdd2 = rdd1.map( (x: String) => (x, x.toInt) )
          val rdd3 = rdd2.reduceByKey((x: Int, y: Int) => if(x > y) x else y)
          rdd3
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.String), BaseType(BaseTypesEnum.Int))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.String), BaseType(BaseTypesEnum.Int))))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val originals = programSource.programs.head.transformations

    assert(SparkRDDUnaryTransformationDeletion.isApplicable(originals))

    val mutants = SparkRDDUnaryTransformationDeletion.generateMutants(originals, idGenerator)

    assert(mutants.size == 1)

    val mutant1 = mutants(0)

    assert(mutant1.mutationOperator == MutationOperatorsEnum.UTD)

    assert(mutant1.original.size == 1)
    assert(originals.contains(mutant1.original.head))
    assert(mutant1.mutated.size == 1)
    assert(!originals.contains(mutant1.mutated.head))

    assert(mutant1.mutated.head.id == mutant1.original.head.id)
    assert(mutant1.mutated.head.edges == mutant1.original.head.edges)

    assert(mutant1.mutated.head.name != mutant1.original.head.name)
    assert(mutant1.mutated.head.name == "reduceByKeyDeletion")

    assert(mutant1.mutated.head.source != mutant1.original.head.source)
    assert(mutant1.original.head.source.isEqual(q"val rdd3 = rdd2.reduceByKey((x: Int, y: Int) => if(x > y) x else y)"))
    assert(mutant1.mutated.head.source.isEqual(q"val rdd3 = rdd2"))

    assert(mutant1.mutated.head.params != mutant1.original.head.params)
    assert(mutant1.mutated.head.params.isEmpty)
  }

  test("Test Case 5 - Three Not Applicable Transformations") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) : RDD[Int] = {
          val rdd2 = rdd1.map( (x: String) => x.toInt )
          val rdd3 = rdd1.map( (x: String) => x.toInt * x.toInt )
          val rdd4 = rdd3.intersection(rdd2)
          rdd4
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val originals = programSource.programs.head.transformations

    assert(!SparkRDDUnaryTransformationDeletion.isApplicable(originals))

    val mutants = SparkRDDUnaryTransformationDeletion.generateMutants(originals, idGenerator)

    assert(mutants.size == 0)
  }

}