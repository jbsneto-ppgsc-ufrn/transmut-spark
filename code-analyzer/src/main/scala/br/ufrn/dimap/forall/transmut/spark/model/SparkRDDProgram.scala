package br.ufrn.dimap.forall.transmut.spark.model

import scala.collection.mutable.ListBuffer
import scala.meta.Tree
import scala.meta.contrib._

import br.ufrn.dimap.forall.transmut.model.Program

case class SparkRDDProgram(override val id: Long, override val name: String, override val tree: Tree, override val programSource: SparkRDDProgramSource) extends Program {

  private var _datasets: ListBuffer[SparkRDD] = scala.collection.mutable.ListBuffer.empty[SparkRDD]
  private var _transformations: ListBuffer[SparkRDDTransformation] = scala.collection.mutable.ListBuffer.empty[SparkRDDTransformation]
  private var _edges: ListBuffer[SparkRDDEdge] = scala.collection.mutable.ListBuffer.empty[SparkRDDEdge]
  
  override def datasets = _datasets.toList

  override def transformations = _transformations.toList

  override def edges = _edges.toList
  
  def addDataset(dataset: SparkRDD) {
    _datasets += dataset
  }

  def addTransformation(transformation: SparkRDDTransformation) {
    _transformations += transformation
  }

  def addEdge(edge: SparkRDDEdge) {
    _edges += edge
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