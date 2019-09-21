package br.ufrn.dimap.forall.model

import scala.collection.mutable.ListBuffer
import br.ufrn.dimap.forall.util.LongIdGenerator

case class Dataset(override val id: Long) extends Element(id) {

  def this(id: Long, reference: Reference, source: TreeElement) {
    this(id)
    _reference = reference
    _source = source
  }

  private var _reference: Reference = _
  private var _source: TreeElement = _
  
  private var _edges : ListBuffer[Edge] = scala.collection.mutable.ListBuffer.empty[Edge]

  def reference = _reference
  def reference_=(reference: Reference) {
    _reference = reference
  }

  def name = reference.name
  def datasetType = reference.valueType

  def source = _source
  def source_=(source: TreeElement) {
    _source = source
  }

  def edges = _edges.toList
  
  def addEdge(edge : Edge) {
    _edges += edge
  }
  
  def incomingEdges = _edges.filter(e => e.direction == DirectionsEnum.TransformationToDataset).toList
  def outgoingEdges = _edges.filter(e => e.direction == DirectionsEnum.DatasetToTransformation).toList
  
  def incomingTransformations = incomingEdges.map(e => e.transformation)
  def outgoingTransformations = outgoingEdges.map(e => e.transformation)
  
  def isInputDataset = incomingEdges.isEmpty
  def isOutputDataset = outgoingEdges.isEmpty
  
}

object Dataset {
  def apply(id: Long, reference: Reference, source: TreeElement) = new Dataset(id, reference, source)
}