package br.ufrn.dimap.forall.transmut.spark.model

import scala.collection.mutable.ListBuffer

import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.model.Program
import scala.meta.Tree
import br.ufrn.dimap.forall.transmut.model.Element
import scala.meta.contrib._

case class SparkRDDProgramSource(override val id: Long) extends ProgramSource {

  def this(id: Long, t: Tree) {
    this(id)
    _tree = t
  }

  private var _programs: ListBuffer[SparkRDDProgram] = scala.collection.mutable.ListBuffer.empty[SparkRDDProgram]

  private var _tree: Tree = _

  override def tree = _tree

  def tree_=(t: Tree) {
    _tree = t
  }

  override def programs = _programs.toList

  def addProgram(p: SparkRDDProgram) {
    _programs += p
  }
  
  def removeProgram(p: SparkRDDProgram) {
    val index = _programs.indexOf(p)
    _programs.remove(index)
  }

  override def copy(id: Long = this.id, tree: Tree = this.tree, programs: List[Program] = this.programs): ProgramSource = {
    val copyProgramSoucer = SparkRDDProgramSource(id)
    copyProgramSoucer.tree = tree
    programs.foreach(p => copyProgramSoucer.addProgram(p.copy().asInstanceOf[SparkRDDProgram]))
    copyProgramSoucer
  }

  override def equals(that: Any): Boolean = that match {
    case that: SparkRDDProgramSource => {
      that.id == id &&
        that.tree.isEqual(tree) &&
        that.programs == programs
    }
    case _ => false
  }

}

object SparkRDDProgramSource {
  def apply(id: Long, tree: Tree) = new SparkRDDProgramSource(id, tree)
}