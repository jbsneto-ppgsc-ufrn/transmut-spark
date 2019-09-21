package br.ufrn.dimap.forall.model

import scala.meta._
import scala.collection.mutable._

case class Program(override val id: Long, name: String, tree: Tree) extends Element(id) {

  private var _treeElements: ListBuffer[TreeElement] = scala.collection.mutable.ListBuffer.empty[TreeElement]
  private var _datasets: ListBuffer[Dataset] = scala.collection.mutable.ListBuffer.empty[Dataset]
  private var _transformations: ListBuffer[Transformation] = scala.collection.mutable.ListBuffer.empty[Transformation]
  private var _edges: ListBuffer[Edge] = scala.collection.mutable.ListBuffer.empty[Edge]

  def treeElements = _treeElements.toList
  def datasets = _datasets.toList
  def transformations = _transformations.toList
  def edges = _edges.toList
  def inputDatasets = datasets.filter(d => d.isInputDataset)
  def outputDatasets = datasets.filter(d => d.isOutputDataset)

  def addTreeElement(treeElement: TreeElement) {
    _treeElements += treeElement
  }

  def addDataset(dataset: Dataset) {
    _datasets += dataset
  }

  def addTransformation(transformation: Transformation) {
    _transformations += transformation
  }

  def addEdge(edge: Edge) {
    _edges += edge
  }

  def datasetByReferenceName(name: String): Option[Dataset] = datasets.filter(d => d.reference.name == name).headOption

  def isDatasetByReferenceNameDefined(name: String) = datasetByReferenceName(name).isDefined

}