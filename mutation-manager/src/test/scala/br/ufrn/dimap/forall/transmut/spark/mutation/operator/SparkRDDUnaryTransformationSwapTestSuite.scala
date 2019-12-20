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

class SparkRDDUnaryTransformationSwapTestSuite extends FunSuite {

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

    assert(SparkRDDUnaryTransformationSwap.isApplicable(originals))

    val mutants = SparkRDDUnaryTransformationSwap.generateMutants(originals, idGenerator)

    assert(mutants.size == 1)

    val mutant1 = mutants(0)

    assert(mutant1.mutationOperator == MutationOperatorsEnum.UTS)

    assert(mutant1.original.size == 2)
    assert(originals.contains(mutant1.original(0)))
    assert(originals.contains(mutant1.original(1)))
    assert(mutant1.original(0) != mutant1.original(1))

    assert(mutant1.mutated.size == 2)
    assert(!originals.contains(mutant1.mutated(0)))
    assert(!originals.contains(mutant1.mutated(1)))

    assert(mutant1.mutated(0).id == mutant1.original(0).id)
    assert(mutant1.mutated(0).id != mutant1.original(1).id)
    assert(mutant1.mutated(1).id != mutant1.original(0).id)
    assert(mutant1.mutated(1).id == mutant1.original(1).id)
    assert(mutant1.mutated(0).edges == mutant1.original(0).edges)
    assert(mutant1.mutated(0).edges != mutant1.original(1).edges)
    assert(mutant1.mutated(1).edges != mutant1.original(0).edges)
    assert(mutant1.mutated(1).edges == mutant1.original(1).edges)

    assert(mutant1.mutated(0).name != mutant1.original(0).name)
    assert(mutant1.mutated(0).name != mutant1.original(1).name)
    assert(mutant1.mutated(1).name != mutant1.original(0).name)
    assert(mutant1.mutated(1).name != mutant1.original(1).name)
    assert(mutant1.mutated(0).name == "mapTofilter")
    assert(mutant1.mutated(1).name == "filterTomap")

    assert(mutant1.mutated(0).source != mutant1.original(0).source)
    assert(mutant1.mutated(0).source != mutant1.original(1).source)
    assert(mutant1.mutated(1).source != mutant1.original(0).source)
    assert(mutant1.mutated(1).source != mutant1.original(1).source)
    assert(mutant1.original(0).source.isEqual(q"val rdd2 = rdd1.map(a => a + 1)"))
    assert(mutant1.original(1).source.isEqual(q"val rdd3 = rdd2.filter(a => a < 100)"))
    assert(mutant1.mutated(0).source.isEqual(q"val rdd2 = rdd1.filter(a => a < 100)"))
    assert(mutant1.mutated(1).source.isEqual(q"val rdd3 = rdd2.map(a => a + 1)"))

    assert(mutant1.mutated(0).params != mutant1.original(0).params)
    assert(mutant1.mutated(0).params == mutant1.original(1).params)
    assert(mutant1.mutated(1).params == mutant1.original(0).params)
    assert(mutant1.mutated(1).params != mutant1.original(1).params)
  }

  test("Test Case 2 - Three Applicable Transformations") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(input: RDD[Int]) = {
          val even = input.filter(x => x % 2 == 0)
          val square = even.map(x => x * x)
          val sorted = square.sortBy(x => x)
          sorted
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("input" -> ParameterReference("input", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("even" -> ValReference("even", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("square" -> ValReference("square", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("sorted" -> ValReference("sorted", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val originals = programSource.programs.head.transformations

    assert(SparkRDDUnaryTransformationSwap.isApplicable(originals))

    val mutants = SparkRDDUnaryTransformationSwap.generateMutants(originals, idGenerator)

    assert(mutants.size == 3)

    val mutant1 = mutants(0)
    val mutant2 = mutants(1)
    val mutant3 = mutants(2)

    // Mutant 1
    assert(mutant1.mutationOperator == MutationOperatorsEnum.UTS)

    assert(mutant1.original.size == 2)
    assert(originals.contains(mutant1.original(0)))
    assert(originals.contains(mutant1.original(1)))
    assert(mutant1.original(0) != mutant1.original(1))

    assert(mutant1.mutated.size == 2)
    assert(!originals.contains(mutant1.mutated(0)))
    assert(!originals.contains(mutant1.mutated(1)))

    assert(mutant1.mutated(0).id == mutant1.original(0).id)
    assert(mutant1.mutated(0).id != mutant1.original(1).id)
    assert(mutant1.mutated(1).id != mutant1.original(0).id)
    assert(mutant1.mutated(1).id == mutant1.original(1).id)
    assert(mutant1.mutated(0).edges == mutant1.original(0).edges)
    assert(mutant1.mutated(0).edges != mutant1.original(1).edges)
    assert(mutant1.mutated(1).edges != mutant1.original(0).edges)
    assert(mutant1.mutated(1).edges == mutant1.original(1).edges)

    assert(mutant1.mutated(0).name != mutant1.original(0).name)
    assert(mutant1.mutated(0).name != mutant1.original(1).name)
    assert(mutant1.mutated(0).name == "filterTomap")
    assert(mutant1.mutated(1).name == "mapTofilter")

    assert(mutant1.mutated(0).source != mutant1.original(0).source)
    assert(mutant1.mutated(0).source != mutant1.original(1).source)
    assert(mutant1.mutated(1).source != mutant1.original(0).source)
    assert(mutant1.mutated(1).source != mutant1.original(1).source)
    assert(mutant1.original(0).source.isEqual(q"val even = input.filter(x => x % 2 == 0)"))
    assert(mutant1.original(1).source.isEqual(q"val square = even.map(x => x * x)"))
    assert(mutant1.mutated(0).source.isEqual(q"val even = input.map(x => x * x)"))
    assert(mutant1.mutated(1).source.isEqual(q"val square = even.filter(x => x % 2 == 0)"))

    assert(mutant1.mutated(0).params != mutant1.original(0).params)
    assert(mutant1.mutated(0).params == mutant1.original(1).params)
    assert(mutant1.mutated(1).params == mutant1.original(0).params)
    assert(mutant1.mutated(1).params != mutant1.original(1).params)

    // Mutant 2
    assert(mutant2.mutationOperator == MutationOperatorsEnum.UTS)

    assert(mutant2.original.size == 2)
    assert(originals.contains(mutant2.original(0)))
    assert(originals.contains(mutant2.original(1)))
    assert(mutant2.original(0) != mutant2.original(1))

    assert(mutant2.mutated.size == 2)
    assert(!originals.contains(mutant2.mutated(0)))
    assert(!originals.contains(mutant2.mutated(1)))

    assert(mutant2.mutated(0).id == mutant2.original(0).id)
    assert(mutant2.mutated(0).id != mutant2.original(1).id)
    assert(mutant2.mutated(1).id != mutant2.original(0).id)
    assert(mutant2.mutated(1).id == mutant2.original(1).id)
    assert(mutant2.mutated(0).edges == mutant2.original(0).edges)
    assert(mutant2.mutated(0).edges != mutant2.original(1).edges)
    assert(mutant2.mutated(1).edges != mutant2.original(0).edges)
    assert(mutant2.mutated(1).edges == mutant2.original(1).edges)

    assert(mutant2.mutated(0).name != mutant2.original(0).name)
    assert(mutant2.mutated(0).name != mutant2.original(1).name)
    assert(mutant2.mutated(1).name != mutant2.original(0).name)
    assert(mutant2.mutated(1).name != mutant2.original(1).name)
    assert(mutant2.mutated(0).name == "filterTosortBy")
    assert(mutant2.mutated(1).name == "sortByTofilter")

    assert(mutant2.mutated(0).source != mutant2.original(0).source)
    assert(mutant2.mutated(0).source != mutant2.original(1).source)
    assert(mutant2.mutated(1).source != mutant2.original(0).source)
    assert(mutant2.mutated(1).source != mutant2.original(1).source)
    assert(mutant2.original(0).source.isEqual(q"val even = input.filter(x => x % 2 == 0)"))
    assert(mutant2.original(1).source.isEqual(q"val sorted = square.sortBy(x => x)"))
    assert(mutant2.mutated(0).source.isEqual(q"val even = input.sortBy(x => x)"))
    assert(mutant2.mutated(1).source.isEqual(q"val sorted = square.filter(x => x % 2 == 0)"))

    assert(mutant2.mutated(0).params != mutant2.original(0).params)
    assert(mutant2.mutated(0).params == mutant2.original(1).params)
    assert(mutant2.mutated(1).params == mutant2.original(0).params)
    assert(mutant2.mutated(1).params != mutant2.original(1).params)

    // Mutant 3
    assert(mutant3.mutationOperator == MutationOperatorsEnum.UTS)

    assert(mutant3.original.size == 2)
    assert(originals.contains(mutant3.original(0)))
    assert(originals.contains(mutant3.original(1)))
    assert(mutant3.original(0) != mutant3.original(1))

    assert(mutant3.mutated.size == 2)
    assert(!originals.contains(mutant3.mutated(0)))
    assert(!originals.contains(mutant3.mutated(1)))

    assert(mutant3.mutated(0).id == mutant3.original(0).id)
    assert(mutant3.mutated(0).id != mutant3.original(1).id)
    assert(mutant3.mutated(1).id != mutant3.original(0).id)
    assert(mutant3.mutated(1).id == mutant3.original(1).id)
    assert(mutant3.mutated(0).edges == mutant3.original(0).edges)
    assert(mutant3.mutated(0).edges != mutant3.original(1).edges)
    assert(mutant3.mutated(1).edges != mutant3.original(0).edges)
    assert(mutant3.mutated(1).edges == mutant3.original(1).edges)

    assert(mutant3.mutated(0).name != mutant3.original(0).name)
    assert(mutant3.mutated(0).name != mutant3.original(1).name)
    assert(mutant3.mutated(1).name != mutant3.original(0).name)
    assert(mutant3.mutated(1).name != mutant3.original(1).name)
    assert(mutant3.mutated(0).name == "mapTosortBy")
    assert(mutant3.mutated(1).name == "sortByTomap")

    assert(mutant3.mutated(0).source != mutant3.original(0).source)
    assert(mutant3.mutated(0).source != mutant3.original(1).source)
    assert(mutant3.mutated(1).source != mutant3.original(0).source)
    assert(mutant3.mutated(1).source != mutant3.original(1).source)
    assert(mutant3.original(0).source.isEqual(q"val square = even.map(x => x * x)"))
    assert(mutant3.original(1).source.isEqual(q"val sorted = square.sortBy(x => x)"))
    assert(mutant3.mutated(0).source.isEqual(q"val square = even.sortBy(x => x)"))
    assert(mutant3.mutated(1).source.isEqual(q"val sorted = square.map(x => x * x)"))

    assert(mutant3.mutated(0).params != mutant3.original(0).params)
    assert(mutant3.mutated(0).params == mutant3.original(1).params)
    assert(mutant3.mutated(1).params == mutant3.original(0).params)
    assert(mutant3.mutated(1).params != mutant3.original(1).params)
  }

  test("Test Case 3 - Two Applicable Transformations (One Without Parameters)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map(a => a + 1)
          val rdd3 = rdd2.distinct
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

    assert(SparkRDDUnaryTransformationSwap.isApplicable(originals))

    val mutants = SparkRDDUnaryTransformationSwap.generateMutants(originals, idGenerator)

    assert(mutants.size == 1)

    val mutant1 = mutants(0)

    assert(mutant1.mutationOperator == MutationOperatorsEnum.UTS)

    assert(mutant1.original.size == 2)
    assert(originals.contains(mutant1.original(0)))
    assert(originals.contains(mutant1.original(1)))
    assert(mutant1.original(0) != mutant1.original(1))

    assert(mutant1.mutated.size == 2)
    assert(!originals.contains(mutant1.mutated(0)))
    assert(!originals.contains(mutant1.mutated(1)))

    assert(mutant1.mutated(0).id == mutant1.original(0).id)
    assert(mutant1.mutated(0).id != mutant1.original(1).id)
    assert(mutant1.mutated(1).id != mutant1.original(0).id)
    assert(mutant1.mutated(1).id == mutant1.original(1).id)
    assert(mutant1.mutated(0).edges == mutant1.original(0).edges)
    assert(mutant1.mutated(0).edges != mutant1.original(1).edges)
    assert(mutant1.mutated(1).edges != mutant1.original(0).edges)
    assert(mutant1.mutated(1).edges == mutant1.original(1).edges)

    assert(mutant1.mutated(0).name != mutant1.original(0).name)
    assert(mutant1.mutated(0).name != mutant1.original(1).name)
    assert(mutant1.mutated(1).name != mutant1.original(0).name)
    assert(mutant1.mutated(1).name != mutant1.original(1).name)
    assert(mutant1.mutated(0).name == "mapTodistinct")
    assert(mutant1.mutated(1).name == "distinctTomap")

    assert(mutant1.mutated(0).source != mutant1.original(0).source)
    assert(mutant1.mutated(0).source != mutant1.original(1).source)
    assert(mutant1.mutated(1).source != mutant1.original(0).source)
    assert(mutant1.mutated(1).source != mutant1.original(1).source)
    assert(mutant1.original(0).source.isEqual(q"val rdd2 = rdd1.map(a => a + 1)"))
    assert(mutant1.original(1).source.isEqual(q"val rdd3 = rdd2.distinct"))
    assert(mutant1.mutated(0).source.isEqual(q"val rdd2 = rdd1.distinct"))
    assert(mutant1.mutated(1).source.isEqual(q"val rdd3 = rdd2.map(a => a + 1)"))

    assert(mutant1.mutated(0).params != mutant1.original(0).params)
    assert(mutant1.mutated(0).params == mutant1.original(1).params)
    assert(mutant1.mutated(1).params == mutant1.original(0).params)
    assert(mutant1.mutated(1).params != mutant1.original(1).params)
  }

  test("Test Case 4 - Two Applicable Transformations (One Without Parameters) and One Not Applicable Binary Transformation") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map(a => a * a)
          val rdd3 = rdd1.union(rdd2)
          val rdd4 = rdd3.distinct()
          rdd4
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val originals = programSource.programs.head.transformations

    assert(SparkRDDUnaryTransformationSwap.isApplicable(originals))

    val mutants = SparkRDDUnaryTransformationSwap.generateMutants(originals, idGenerator)

    assert(mutants.size == 1)

    val mutant1 = mutants(0)

    assert(mutant1.mutationOperator == MutationOperatorsEnum.UTS)

    assert(mutant1.original.size == 2)
    assert(originals.contains(mutant1.original(0)))
    assert(originals.contains(mutant1.original(1)))
    assert(mutant1.original(0) != mutant1.original(1))

    assert(mutant1.mutated.size == 2)
    assert(!originals.contains(mutant1.mutated(0)))
    assert(!originals.contains(mutant1.mutated(1)))

    assert(mutant1.mutated(0).id == mutant1.original(0).id)
    assert(mutant1.mutated(0).id != mutant1.original(1).id)
    assert(mutant1.mutated(1).id != mutant1.original(0).id)
    assert(mutant1.mutated(1).id == mutant1.original(1).id)
    assert(mutant1.mutated(0).edges == mutant1.original(0).edges)
    assert(mutant1.mutated(0).edges != mutant1.original(1).edges)
    assert(mutant1.mutated(1).edges != mutant1.original(0).edges)
    assert(mutant1.mutated(1).edges == mutant1.original(1).edges)

    assert(mutant1.mutated(0).name != mutant1.original(0).name)
    assert(mutant1.mutated(0).name != mutant1.original(1).name)
    assert(mutant1.mutated(1).name != mutant1.original(0).name)
    assert(mutant1.mutated(1).name != mutant1.original(1).name)
    assert(mutant1.mutated(0).name == "mapTodistinct")
    assert(mutant1.mutated(1).name == "distinctTomap")

    assert(mutant1.mutated(0).source != mutant1.original(0).source)
    assert(mutant1.mutated(0).source != mutant1.original(1).source)
    assert(mutant1.mutated(1).source != mutant1.original(0).source)
    assert(mutant1.mutated(1).source != mutant1.original(1).source)
    assert(mutant1.original(0).source.isEqual(q"val rdd2 = rdd1.map(a => a * a)"))
    assert(mutant1.original(1).source.isEqual(q"val rdd4 = rdd3.distinct()"))
    assert(mutant1.mutated(0).source.isEqual(q"val rdd2 = rdd1.distinct()"))
    assert(mutant1.mutated(1).source.isEqual(q"val rdd4 = rdd3.map(a => a * a)"))

    assert(mutant1.mutated(0).params != mutant1.original(0).params)
    assert(mutant1.mutated(0).params == mutant1.original(1).params)
    assert(mutant1.mutated(1).params == mutant1.original(0).params)
    assert(mutant1.mutated(1).params != mutant1.original(1).params)
  }

  test("Test Case 5 - Not Applicable Transformations") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[String]) : RDD[Int] = {
          val rdd2 = rdd1.map( (x: String) => x.toInt )
          val rdd3 = rdd2.filter(x => x % 2 == 0)
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

    assert(!SparkRDDUnaryTransformationSwap.isApplicable(originals))

    val mutants = SparkRDDUnaryTransformationSwap.generateMutants(originals, idGenerator)

    assert(mutants.size == 0)
  }

}