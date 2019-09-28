package br.ufrn.dimap.forall.transmut.model

import scala.meta._
import scala.collection.mutable._

trait ProgramSource extends Element {

  def tree: Tree

  def programs: List[Program]

}