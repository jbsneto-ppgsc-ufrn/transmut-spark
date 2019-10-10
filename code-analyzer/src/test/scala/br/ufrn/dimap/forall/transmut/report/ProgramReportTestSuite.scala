package br.ufrn.dimap.forall.transmut.report

import org.scalatest._

import java.nio.file.{ Paths, Files }

import scala.meta._

import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import br.ufrn.dimap.forall.transmut.model._

class ProgramReportTestSuite extends FunSuite with BeforeAndAfter {

  before {
    val path = Paths.get("./bin/program.html")
    if (Files.exists(path) && Files.isRegularFile(path)) {
      Files.delete(path)
    }
  }

  test("Test Case 1 - Verify if the html report file is created") {

    val tree: Tree = q"""
      import org.apache.spark.rdd.RDD

      object SparkProgram {
      
        def program(rdd1: RDD[Int]) = {
          val rdd2 = rdd1.map(a => a * 2)
          val rdd3 = rdd2.filter(a => a < 100)
          val result = rdd3.reduce((a, b) => a + b)
          result
        }
        
      }"""

    val refenceTypes = scala.collection.mutable.Map[String, Reference]()
    refenceTypes += ("rdd1" -> ParameterReference("rdd1", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd2" -> ValReference("rdd2", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("rdd3" -> ValReference("rdd3", ParameterizedType("org/apache/spark/rdd/RDD#", List(BaseType(BaseTypesEnum.Int)))))
    refenceTypes += ("result" -> ValReference("result", BaseType(BaseTypesEnum.Int)))

    val programNames = List("program")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes.toMap)

    assert(programSource.programs.size == 1)

    val program = programSource.programs.head

    ProgramReport.generateProgramHtmlReportFile(program, "./bin/")

    val path = Paths.get("./bin/program.html")
    assert(Files.exists(path) && Files.isRegularFile(path))

  }

  after {
    val path = Paths.get("./bin/program.html")
    if (Files.exists(path) && Files.isRegularFile(path)) {
      Files.delete(path)
    }
  }

}