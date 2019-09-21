package br.ufrn.dimap.forall.model

import scala.collection.mutable.ListBuffer
import scala.meta.Tree

abstract class Transformation(override val id: Long) extends Element(id) {
  def name : String
  def source : TreeElement
  def edges : List[Edge]
  def addEdge(edge: Edge) : Unit
  def incomingEdges = edges.filter(e => e.direction == DirectionsEnum.DatasetToTransformation).toList
  def outgoingEdges = edges.filter(e => e.direction == DirectionsEnum.TransformationToDataset).toList
  def incomingDatasets = incomingEdges.map(e => e.dataset)
  def outgoingDatasets = outgoingEdges.map(e => e.dataset)
  def inputTypes = incomingEdges.map(e => e.dataset.datasetType)
  def outputTypes = outgoingEdges.map(e => e.dataset.datasetType)
}