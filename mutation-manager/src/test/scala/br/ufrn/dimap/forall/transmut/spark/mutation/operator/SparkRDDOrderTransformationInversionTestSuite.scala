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

class SparkRDDOrderTransformationInversionTestSuite extends FunSuite {

  test("Test Case 1 - Applicable sortBy transformation with a function parameter but no order parameter") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortBy((x: Int) => x)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationInversion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationInversion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head

    assert(mutant.mutationOperator == MutationOperatorsEnum.OTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "sortByInverted")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortBy((x: Int) => x)"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1.sortBy((x: Int) => x, false)"))

    assert(mutant.original.params.size == 1)
    assert(mutant.original.params.head.isEqual(q"(x: Int) => x"))
    assert(mutant.mutated.params.size == 2)
    assert(mutant.mutated.params(0).isEqual(q"(x: Int) => x"))
    assert(mutant.mutated.params(1).isEqual(q"false"))
  }

  test("Test Case 2 - Applicable sortBy transformation with a function parameter and an order parameter (true)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortBy((x: Int) => x, true)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationInversion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationInversion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head

    assert(mutant.mutationOperator == MutationOperatorsEnum.OTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "sortByInverted")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortBy((x: Int) => x, true)"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1.sortBy((x: Int) => x, false)"))

    assert(mutant.original.params.size == 2)
    assert(mutant.original.params(0).isEqual(q"(x: Int) => x"))
    assert(mutant.original.params(1).isEqual(q"true"))
    assert(mutant.mutated.params.size == 2)
    assert(mutant.mutated.params(0).isEqual(q"(x: Int) => x"))
    assert(mutant.mutated.params(1).isEqual(q"false"))
  }

  test("Test Case 3 - Applicable sortBy transformation with a function parameter and an order parameter (false)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortBy((x: Int) => x, false)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationInversion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationInversion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head

    assert(mutant.mutationOperator == MutationOperatorsEnum.OTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "sortByInverted")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortBy((x: Int) => x, false)"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1.sortBy((x: Int) => x, true)"))

    assert(mutant.original.params.size == 2)
    assert(mutant.original.params(0).isEqual(q"(x: Int) => x"))
    assert(mutant.original.params(1).isEqual(q"false"))
    assert(mutant.mutated.params.size == 2)
    assert(mutant.mutated.params(0).isEqual(q"(x: Int) => x"))
    assert(mutant.mutated.params(1).isEqual(q"true"))
  }

  test("Test Case 4 - Applicable sortBy transformation with a function parameter and an order parameter (general)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        val order = true
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortBy((x: Int) => x, order)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationInversion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationInversion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head

    assert(mutant.mutationOperator == MutationOperatorsEnum.OTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "sortByInverted")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortBy((x: Int) => x, order)"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1.sortBy((x: Int) => x, !(order))"))

    assert(mutant.original.params.size == 2)
    assert(mutant.original.params(0).isEqual(q"(x: Int) => x"))
    assert(mutant.original.params(1).isEqual(q"order"))
    assert(mutant.mutated.params.size == 2)
    assert(mutant.mutated.params(0).isEqual(q"(x: Int) => x"))
    assert(mutant.mutated.params(1).isEqual(q"!(order)"))
  }

  test("Test Case 5 - Applicable sortByKey transformation without parentheses") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortByKey
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationInversion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationInversion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head

    assert(mutant.mutationOperator == MutationOperatorsEnum.OTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "sortByKeyInverted")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortByKey"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1.sortByKey(false)"))

    assert(mutant.original.params.isEmpty)
    assert(mutant.mutated.params.size == 1)
    assert(mutant.mutated.params(0).isEqual(q"false"))
  }

  test("Test Case 6 - Applicable sortByKey transformation with order parameter (false)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortByKey(false)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationInversion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationInversion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head

    assert(mutant.mutationOperator == MutationOperatorsEnum.OTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "sortByKeyInverted")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortByKey(false)"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1.sortByKey(true)"))

    assert(mutant.mutated.params != mutant.original.params)
    assert(mutant.original.params.size == 1)
    assert(mutant.original.params(0).isEqual(q"false"))
    assert(mutant.mutated.params.size == 1)
    assert(mutant.mutated.params(0).isEqual(q"true"))
  }

  test("Test Case 7 - Applicable sortByKey transformation with order parameter (true)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortByKey(true)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationInversion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationInversion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head

    assert(mutant.mutationOperator == MutationOperatorsEnum.OTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "sortByKeyInverted")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortByKey(true)"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1.sortByKey(false)"))

    assert(mutant.mutated.params != mutant.original.params)
    assert(mutant.original.params.size == 1)
    assert(mutant.original.params(0).isEqual(q"true"))
    assert(mutant.mutated.params.size == 1)
    assert(mutant.mutated.params(0).isEqual(q"false"))
  }

  test("Test Case 8 - Applicable sortByKey transformation with order parameter (general)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        val order = true
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortByKey(order)
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationInversion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationInversion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head

    assert(mutant.mutationOperator == MutationOperatorsEnum.OTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "sortByKeyInverted")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortByKey(order)"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1.sortByKey(!(order))"))

    assert(mutant.mutated.params != mutant.original.params)
    assert(mutant.original.params.size == 1)
    assert(mutant.original.params(0).isEqual(q"order"))
    assert(mutant.mutated.params.size == 1)
    assert(mutant.mutated.params(0).isEqual(q"!(order)"))
  }

  test("Test Case 9 - Applicable sortByKey transformation with parentheses and without parameters") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.sortByKey()
          rdd2
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val original = programSource.programs.head.transformations.head

    assert(SparkRDDOrderTransformationInversion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationInversion.generateMutants(original, idGenerator)

    assert(mutants.size == 1)

    val mutant = mutants.head

    assert(mutant.mutationOperator == MutationOperatorsEnum.OTI)

    assert(mutant.original == original)
    assert(mutant.mutated != original)

    assert(mutant.mutated.id == mutant.original.id)
    assert(mutant.mutated.edges == mutant.original.edges)

    assert(mutant.mutated.name != mutant.original.name)
    assert(mutant.mutated.name == "sortByKeyInverted")

    assert(mutant.mutated.source != mutant.original.source)
    assert(mutant.original.source.isEqual(q"val rdd2 = rdd1.sortByKey()"))
    assert(mutant.mutated.source.isEqual(q"val rdd2 = rdd1.sortByKey(false)"))

    assert(mutant.original.params.isEmpty)
    assert(mutant.mutated.params.size == 1)
    assert(mutant.mutated.params(0).isEqual(q"false"))
  }

  test("Test Case 10 - Not Applicable Transformation") {

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

    assert(!SparkRDDOrderTransformationInversion.isApplicable(original))

    val mutants = SparkRDDOrderTransformationInversion.generateMutants(original, idGenerator)

    assert(mutants.isEmpty)
  }

}