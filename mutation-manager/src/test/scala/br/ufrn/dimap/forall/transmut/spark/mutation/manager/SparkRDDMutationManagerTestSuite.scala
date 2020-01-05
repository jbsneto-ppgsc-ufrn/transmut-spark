package br.ufrn.dimap.forall.transmut.spark.mutation.manager

import scala.meta._
import scala.meta.contrib._

import org.scalatest.FunSuite

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator

class SparkRDDMutationManagerTestSuite extends FunSuite {

  test("Test Case 1 - Dataflow Mutation Operators - Unary Transformations (UTS, UTR, UTD)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map((a: Int) => a + 1)
          val rdd3 = rdd2.filter((a: Int) => a < 100)
          rdd3
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(UTS, UTR, UTD))

    assert(programSourceMutants.size == 5)

    val mutantUTS = programSourceMutants(0)
    val mutantUTR1 = programSourceMutants(1)
    val mutantUTR2 = programSourceMutants(2)
    val mutantUTD1 = programSourceMutants(3)
    val mutantUTD2 = programSourceMutants(4)

    assert(mutantUTS.original == programSource)
    val treeUTS: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.filter((a: Int) => a < 100)
          val rdd3 = rdd2.map((a: Int) => a + 1)
          rdd3
        }
      }"""
    assert(mutantUTS.mutationOperator == UTS)
    assert(mutantUTS.mutated.tree.isEqual(treeUTS))

    assert(mutantUTR1.original == programSource)
    val treeUTR1: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.filter((a: Int) => a < 100)
          val rdd3 = rdd2.filter((a: Int) => a < 100)
          rdd3
        }
      }"""
    assert(mutantUTR1.mutationOperator == UTR)
    assert(mutantUTR1.mutated.tree.isEqual(treeUTR1))

    assert(mutantUTR2.original == programSource)
    val treeUTR2: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map((a: Int) => a + 1)
          val rdd3 = rdd2.map((a: Int) => a + 1)
          rdd3
        }
      }"""
    assert(mutantUTR2.mutationOperator == UTR)
    assert(mutantUTR2.mutated.tree.isEqual(treeUTR2))

    assert(mutantUTD1.original == programSource)
    val treeUTD1: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1
          val rdd3 = rdd2.filter((a: Int) => a < 100)
          rdd3
        }
      }"""
    assert(mutantUTD1.mutationOperator == UTD)
    assert(mutantUTD1.mutated.tree.isEqual(treeUTD1))

    assert(mutantUTD2.original == programSource)
    val treeUTD2: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map((a: Int) => a + 1)
          val rdd3 = rdd2
          rdd3
        }
      }"""
    assert(mutantUTD2.mutationOperator == UTD)
    assert(mutantUTD2.mutated.tree.isEqual(treeUTD2))
  }

  test("Test Case 2 - Dataflow Mutation Operators - Binary Transformations (BTS, BTR)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.subtract(rdd2)
          val rdd4 = rdd3.union(rdd2)
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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(BTS, BTR))

    assert(programSourceMutants.size == 3)

    val mutantBTS = programSourceMutants(0)
    val mutantBTR1 = programSourceMutants(1)
    val mutantBTR2 = programSourceMutants(2)

    assert(mutantBTS.original == programSource)
    val treeBTS: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.union(rdd2)
          val rdd4 = rdd3.subtract(rdd2)
          rdd4
        }
      }"""
    assert(mutantBTS.mutationOperator == BTS)
    assert(mutantBTS.mutated.tree.isEqual(treeBTS))

    assert(mutantBTR1.original == programSource)
    val treeBTR1: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.union(rdd2)
          val rdd4 = rdd3.union(rdd2)
          rdd4
        }
      }"""
    assert(mutantBTR1.mutationOperator == BTR)
    assert(mutantBTR1.mutated.tree.isEqual(treeBTR1))

    assert(mutantBTR2.original == programSource)
    val treeBTR2: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.subtract(rdd2)
          val rdd4 = rdd3.subtract(rdd2)
          rdd4
        }
      }"""
    assert(mutantBTR2.mutationOperator == BTR)
    assert(mutantBTR2.mutated.tree.isEqual(treeBTR2))
  }

  test("Test Case 3 - Transformation Mutation Operators - Unary Transformations - Filter, Map, Sort (FTD, MTR, OTD)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          rdd4
        }
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd4" -> ValReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(FTD, MTR, OTD))

    assert(programSourceMutants.size == 3)

    val mutantFTD = programSourceMutants(0)
    val mutantMTR = programSourceMutants(1)
    val mutantOTD = programSourceMutants(2)

    assert(mutantFTD.original == programSource)
    val treeFTD: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3.sortBy((x: String) => x)
          rdd4
        }
      }"""
    assert(mutantFTD.mutationOperator == FTD)
    assert(mutantFTD.mutated.tree.isEqual(treeFTD))

    assert(mutantMTR.original == programSource)
    val treeMTR: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((inputParameter: String) => {
            val originalFunction = (x: String) => x + "test"
            val originalValue = originalFunction(inputParameter)
            ""
          })
          val rdd4 = rdd3.sortBy((x: String) => x)
          rdd4
        }
      }"""
    assert(mutantMTR.mutationOperator == MTR)
    assert(mutantMTR.mutated.tree.isEqual(treeMTR))

    assert(mutantOTD.original == programSource)
    val treeOTD: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[String]) = {
          val rdd2 = rdd1.filter((x: String) => !x.isEmpty)
          val rdd3 = rdd2.map((x: String) => x + "test")
          val rdd4 = rdd3
          rdd4
        }
      }"""
    assert(mutantOTD.mutationOperator == OTD)
    assert(mutantOTD.mutated.tree.isEqual(treeOTD))
  }

  test("Test Case 4 - Transformation Mutation Operators - Unary Transformations - Aggregation, Distinct (ATR, DTI)") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(ATR, DTI))

    assert(programSourceMutants.size == 6)

    val mutantATR1 = programSourceMutants(0)
    val mutantATR2 = programSourceMutants(1)
    val mutantATR3 = programSourceMutants(2)
    val mutantATR4 = programSourceMutants(3)
    val mutantATR5 = programSourceMutants(4)
    val mutantDTI = programSourceMutants(5)

    assert(mutantATR1.original == programSource)
    val treeATR1: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => firstParameter)
          rdd2
        }
      }"""
    assert(mutantATR1.mutationOperator == ATR)
    assert(mutantATR1.mutated.tree.isEqual(treeATR1))

    assert(mutantATR2.original == programSource)
    val treeATR2: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => secondParameter)
          rdd2
        }
      }"""
    assert(mutantATR2.mutationOperator == ATR)
    assert(mutantATR2.mutated.tree.isEqual(treeATR2))

    assert(mutantATR3.original == programSource)
    val treeATR3: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => {
            val originalFunction = (x: Double, y: Double) => if(x > y) x else y
            originalFunction(firstParameter, firstParameter)
          })
          rdd2
        }
      }"""
    assert(mutantATR3.mutationOperator == ATR)
    assert(mutantATR3.mutated.tree.isEqual(treeATR3))

    assert(mutantATR4.original == programSource)
    val treeATR4: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => {
            val originalFunction = (x: Double, y: Double) => if(x > y) x else y
            originalFunction(secondParameter, secondParameter)
           })
          rdd2
        }
      }"""
    assert(mutantATR4.mutationOperator == ATR)
    assert(mutantATR4.mutated.tree.isEqual(treeATR4))

    assert(mutantATR5.original == programSource)
    val treeATR5: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.reduceByKey((firstParameter: Double, secondParameter: Double) => {
            val originalFunction = (x: Double, y: Double) => if(x > y) x else y
            originalFunction(secondParameter, firstParameter)
          })
          rdd2
        }
      }"""
    assert(mutantATR5.mutationOperator == ATR)
    assert(mutantATR5.mutated.tree.isEqual(treeATR5))

    assert(mutantDTI.original == programSource)
    val treeDTI: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)]) : RDD[(Long, Double)] = {
          val rdd2 = rdd1.reduceByKey((x: Double, y: Double) => if(x > y) x else y).distinct()
          rdd2
        }
      }"""
    assert(mutantDTI.mutationOperator == DTI)
    assert(mutantDTI.mutated.tree.isEqual(treeDTI))
  }

  test("Test Case 5 - Transformation Mutation Operators - Binary and Unary Transformations - Set, Distinct (STR, DTD)") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(STR, DTD))

    assert(programSourceMutants.size == 6)

    val mutantSTR1 = programSourceMutants(0)
    val mutantSTR2 = programSourceMutants(1)
    val mutantSTR3 = programSourceMutants(2)
    val mutantSTR4 = programSourceMutants(3)
    val mutantSTR5 = programSourceMutants(4)
    val mutantDTD = programSourceMutants(5)

    assert(mutantSTR1.original == programSource)
    val treeSTR1: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.intersection(rdd2)
          val rdd4 = rdd3.distinct
          rdd4
        }
      }"""
    assert(mutantSTR1.mutationOperator == STR)
    assert(mutantSTR1.mutated.tree.isEqual(treeSTR1))

    assert(mutantSTR2.original == programSource)
    val treeSTR2: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.subtract(rdd2)
          val rdd4 = rdd3.distinct
          rdd4
        }
      }"""
    assert(mutantSTR2.mutationOperator == STR)
    assert(mutantSTR2.mutated.tree.isEqual(treeSTR2))

    assert(mutantSTR3.original == programSource)
    val treeSTR3: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1
          val rdd4 = rdd3.distinct
          rdd4
        }
      }"""
    assert(mutantSTR3.mutationOperator == STR)
    assert(mutantSTR3.mutated.tree.isEqual(treeSTR3))

    assert(mutantSTR4.original == programSource)
    val treeSTR4: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd2
          val rdd4 = rdd3.distinct
          rdd4
        }
      }"""
    assert(mutantSTR4.mutationOperator == STR)
    assert(mutantSTR4.mutated.tree.isEqual(treeSTR4))

    assert(mutantSTR5.original == programSource)
    val treeSTR5: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd2.union(rdd1)
          val rdd4 = rdd3.distinct
          rdd4
        }
      }"""
    assert(mutantSTR5.mutationOperator == STR)
    assert(mutantSTR5.mutated.tree.isEqual(treeSTR5))

    assert(mutantDTD.original == programSource)
    val treeDTD: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[Int], rdd2: RDD[Int]) = {
          val rdd3 = rdd1.union(rdd2)
          val rdd4 = rdd3
          rdd4
        }
      }"""
    assert(mutantDTD.mutationOperator == DTD)
    assert(mutantDTD.mutated.tree.isEqual(treeDTD))
  }

  test("Test Case 6 - Transformation Mutation Operators - Binary Transformation - Join (JTR)") {

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

    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(JTR))

    assert(programSourceMutants.size == 3)

    val mutantJTR1 = programSourceMutants(0)
    val mutantJTR2 = programSourceMutants(1)
    val mutantJTR3 = programSourceMutants(2)

    assert(mutantJTR1.original == programSource)
    val treeJTR1: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, String)]) : RDD[(Long, (Double, String))] = {
          val rdd3 = rdd1.leftOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1, tuple._2._2.getOrElse(""))))
          rdd3
        }
      }"""
    assert(mutantJTR1.mutationOperator == JTR)
    assert(mutantJTR1.mutated.tree.isEqual(treeJTR1))

    assert(mutantJTR2.original == programSource)
    val treeJTR2: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, String)]) : RDD[(Long, (Double, String))] = {
          val rdd3 = rdd1.rightOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1.getOrElse(0d), tuple._2._2)))
          rdd3
        }
      }"""
    assert(mutantJTR2.mutationOperator == JTR)
    assert(mutantJTR2.mutated.tree.isEqual(treeJTR2))

    assert(mutantJTR3.original == programSource)
    val treeJTR3: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        def program(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, String)]) : RDD[(Long, (Double, String))] = {
          val rdd3 = rdd1.fullOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1.getOrElse(0d), tuple._2._2.getOrElse(""))))
          rdd3
        }
      }"""
    assert(mutantJTR3.mutationOperator == JTR)
    assert(mutantJTR3.mutated.tree.isEqual(treeJTR3))
  }

}