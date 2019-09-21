package br.ufrn.dimap.forall.model

import scala.meta._
import scala.collection.mutable._

case class ProgramSource(override val id: Long, tree: Tree) extends Element(id) {

  private var _programs: ListBuffer[Program] = scala.collection.mutable.ListBuffer.empty[Program]
  
  def programs = _programs.toList
  
  def addProgram(program : Program) {
    _programs += program
  }

}