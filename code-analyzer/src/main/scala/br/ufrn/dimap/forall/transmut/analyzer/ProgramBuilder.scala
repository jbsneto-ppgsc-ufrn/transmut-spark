package br.ufrn.dimap.forall.transmut.analyzer

import scala.meta._
import scala.meta.Tree

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.util.LongIdGenerator
import br.ufrn.dimap.forall.transmut.spark.model.SparkRDDProgramSource
import java.nio.file.Path

trait ProgramBuilder {

  def buildProgramSources(sources: List[String], programs: List[String], srsDir: Path, semanticdbDir: Path): List[ProgramSource]

}