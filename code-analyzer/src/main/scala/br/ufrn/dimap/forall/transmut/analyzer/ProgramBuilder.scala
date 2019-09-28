package br.ufrn.dimap.forall.transmut.analyzer

import scala.meta._
import scala.meta.Tree

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.util.LongIdGenerator

trait ProgramBuilder {

  def buildProgramSourceFromProgramNames(programNames: List[String], tree: Tree, refenceTypes: Map[String, Reference]): ProgramSource

}