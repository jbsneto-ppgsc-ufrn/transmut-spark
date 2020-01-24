package br.ufrn.dimap.forall.transmut.model

import scala.meta._
import scala.collection.mutable._
import java.nio.file.Path

trait ProgramSource extends Element {
  
  def source: Path

  def tree: Tree

  def programs: List[Program]

  def copy(id: Long = this.id, tree: Tree = this.tree, source: Path = this.source, programs: List[Program] = this.programs) : ProgramSource
  
}