package br.ufrn.dimap.forall.transmut.spark.mutation.manager

import scala.meta._
import scala.meta.contrib._

import org.scalatest.FunSuite

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum._
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator

class SparkRDDMetaMutantBuilderTestSuite extends FunSuite {
  
  test("Test Case 1 - MetaMutantProgram") {

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

    val program = programSource.programs.head

    val programMutants = SparkRDDMutationManager.generateMutantsFromProgram(program, List(JTR))
    
    val metaMutantProgram = SparkRDDMetaMutantBuilder.buildMetaMutantProgramFromMutantPrograms(program, programMutants)
    
    assert(metaMutantProgram.id == program.id)
    assert(metaMutantProgram.mutants == programMutants)
    assert(metaMutantProgram.original == program)
    
    val metaMutantProgramTree = q"""
      def program(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, String)]): RDD[(Long, (Double, String))] = sys.props.get("CURRENT_MUTANT") match {
        case Some("1") => {
          val rdd3 = rdd1.leftOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1, tuple._2._2.getOrElse(""))))
          rdd3
        }
        case Some("2") => {
          val rdd3 = rdd1.rightOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1.getOrElse(0d), tuple._2._2)))
          rdd3
        }
        case Some("3") => {
          val rdd3 = rdd1.fullOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1.getOrElse(0d), tuple._2._2.getOrElse(""))))
          rdd3
        }
        case _ => {
          val rdd3 = rdd1.join(rdd2)
          rdd3
        }
    }"""
    
    assert(metaMutantProgram.mutated.tree.isEqual(metaMutantProgramTree))
  }
  
  test("Test Case 2 - MetaMutantProgramSource (with two programs)") {

    val idGenerator = LongIdGenerator.generator

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        
        def program1(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, String)]) : RDD[(Long, (Double, String))] = {
          val rdd3 = rdd1.join(rdd2)
          rdd3
        }
        
        def program2(rdd4: RDD[String]) = {
          val rdd5 = rdd4.filter((x: String) => !x.isEmpty)
          val rdd6 = rdd5.sortBy((x: String) => x)
          rdd6
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.Double))))))
    refenceTypes += ("rdd2" -> ParameterReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), BaseType(BaseTypesEnum.String))))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(TupleType(BaseType(BaseTypesEnum.Long), TupleType(BaseType(BaseTypesEnum.Double), BaseType(BaseTypesEnum.String)))))))
    refenceTypes += ("rdd4" -> ParameterReference("rdd4", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd5" -> ValReference("rdd5", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))
    refenceTypes += ("rdd6" -> ValReference("rdd6", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.String)))))

    val programNames = List("program1", "program2")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)
    
    val programSourceMutants = SparkRDDMutationManager.generateMutantsFromProgramSource(programSource, List(JTR, FTD, OTD))
    
    val metaMutantProgramSource = SparkRDDMetaMutantBuilder.buildMetaMutantProgramSourceFromMutantProgramSources(programSource, programSourceMutants)
    
    assert(metaMutantProgramSource.id == programSource.id)
    assert(metaMutantProgramSource.mutants == programSourceMutants)
    assert(metaMutantProgramSource.original.programs.size == metaMutantProgramSource.metaMutantPrograms.size)
    
    val metaMutantMutatedTree: Tree = q"""
      import org.apache.spark.rdd.RDD
      object SparkProgram {
        
        def program1(rdd1: RDD[(Long, Double)], rdd2: RDD[(Long, String)]): RDD[(Long, (Double, String))] = sys.props.get("CURRENT_MUTANT") match {
          case Some("1") => {
            val rdd3 = rdd1.leftOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1, tuple._2._2.getOrElse(""))))
            rdd3
          }
          case Some("2") => {
            val rdd3 = rdd1.rightOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1.getOrElse(0d), tuple._2._2)))
            rdd3
          }
          case Some("3") => {
            val rdd3 = rdd1.fullOuterJoin(rdd2).map(tuple => (tuple._1, (tuple._2._1.getOrElse(0d), tuple._2._2.getOrElse(""))))
            rdd3
          }
          case _ => {
            val rdd3 = rdd1.join(rdd2)
            rdd3
          }
        }
        
        def program2(rdd4: RDD[String]) = sys.props.get("CURRENT_MUTANT") match {
          case Some("4") => {
            val rdd5 = rdd4
            val rdd6 = rdd5.sortBy((x: String) => x)
            rdd6
          }
         case Some("5") => {
           val rdd5 = rdd4.filter((x: String) => !x.isEmpty)
           val rdd6 = rdd5
           rdd6
         }
         case _ => {
           val rdd5 = rdd4.filter((x: String) => !x.isEmpty)
           val rdd6 = rdd5.sortBy((x: String) => x)
           rdd6
         }
      }    
    }"""

    assert(metaMutantProgramSource.mutated.tree.isEqual(metaMutantMutatedTree))
  }
  
}