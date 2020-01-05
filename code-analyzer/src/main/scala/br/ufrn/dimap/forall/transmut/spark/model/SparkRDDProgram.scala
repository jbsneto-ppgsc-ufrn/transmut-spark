package br.ufrn.dimap.forall.transmut.spark.model

import scala.collection.mutable.ListBuffer
import scala.meta.Tree
import scala.meta.contrib._

import br.ufrn.dimap.forall.transmut.model.Program
import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.model.Dataset
import br.ufrn.dimap.forall.transmut.model.ProgramSource
import br.ufrn.dimap.forall.transmut.model.Edge

case class SparkRDDProgram(override val id: Long, override val name: String, override val programSource: SparkRDDProgramSource) extends Program {

  def this(id: Long, name: String, tree: Tree, programSource: SparkRDDProgramSource){
    this(id, name, programSource)
    this.tree = tree
  }
  
  private var _tree: Tree = _
  private var _datasets: ListBuffer[SparkRDD] = scala.collection.mutable.ListBuffer.empty[SparkRDD]
  private var _transformations: ListBuffer[SparkRDDTransformation] = scala.collection.mutable.ListBuffer.empty[SparkRDDTransformation]
  private var _edges: ListBuffer[SparkRDDEdge] = scala.collection.mutable.ListBuffer.empty[SparkRDDEdge]

  override def tree = _tree
  
  override def datasets = _datasets.toList

  override def transformations = _transformations.toList

  override def edges = _edges.toList
  
  def tree_=(sourc: Tree) {
    _tree = sourc
  }

  def addDataset(dataset: SparkRDD) {
    _datasets += dataset
  }

  def addTransformation(transformation: SparkRDDTransformation) {
    _transformations += transformation
  }
  
  def removeTransformation(transformation: SparkRDDTransformation) {
    val index = _transformations.indexOf(transformation)
    _transformations.remove(index)
  }

  def addEdge(edge: SparkRDDEdge) {
    _edges += edge
  }

  override def copy(id: Long = this.id, programSource: ProgramSource = this.programSource, name: String = this.name, tree: Tree = this.tree, datasets: List[Dataset] = this.datasets, transformations: List[Transformation] = this.transformations, edges: List[Edge] = this.edges): Program = {
    val copyProgram = SparkRDDProgram(id, name, tree, programSource.asInstanceOf[SparkRDDProgramSource])
    datasets.foreach(d => copyProgram.addDataset(d.copy().asInstanceOf[SparkRDD]))
    transformations.foreach(t => copyProgram.addTransformation(t.copy().asInstanceOf[SparkRDDTransformation]))
    edges.foreach(e => copyProgram.addEdge(e.copy().asInstanceOf[SparkRDDEdge]))
    copyProgram
  }

  override def equals(that: Any): Boolean = that match {
    case that: SparkRDDProgram => {
      that.id == id &&
        that.name == name &&
        that.tree.isEqual(tree) &&
        that.programSource == programSource &&
        that.edges == edges &&
        that.datasets == datasets &&
        that.transformations == transformations
    }
    case _ => false
  }

}

object SparkRDDProgram {
  def apply(id: Long, name: String, tree: Tree, programSource: SparkRDDProgramSource) = new SparkRDDProgram(id, name, tree, programSource)
}