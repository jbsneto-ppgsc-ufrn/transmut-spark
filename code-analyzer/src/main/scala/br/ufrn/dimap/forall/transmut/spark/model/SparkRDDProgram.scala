package br.ufrn.dimap.forall.transmut.spark.model

import scala.collection.mutable.ListBuffer
import scala.meta.Tree

import br.ufrn.dimap.forall.transmut.model._

case class SparkRDDProgram(override val id: Long, override val name: String, override val tree: Tree) extends Program {

//  private var _treeElements: ListBuffer[TreeElement] = scala.collection.mutable.ListBuffer.empty[TreeElement]
  private var _datasets: ListBuffer[SparkRDD] = scala.collection.mutable.ListBuffer.empty[SparkRDD]
  private var _transformations: ListBuffer[SparkRDDTransformation] = scala.collection.mutable.ListBuffer.empty[SparkRDDTransformation]
  private var _edges: ListBuffer[SparkRDDEdge] = scala.collection.mutable.ListBuffer.empty[SparkRDDEdge]

//  override def treeElements = _treeElements.toList

  override def datasets = _datasets.toList

  override def transformations = _transformations.toList

  override def edges = _edges.toList

//  def addTreeElement(treeElement: TreeElement) {
//    _treeElements += treeElement
//  }

  def addDataset(dataset: SparkRDD) {
    _datasets += dataset
  }

  def addTransformation(transformation: SparkRDDTransformation) {
    _transformations += transformation
  }

  def addEdge(edge: SparkRDDEdge) {
    _edges += edge
  }

}