package br.ufrn.dimap.forall.transmut.model

import scala.meta.Tree

trait Dataset extends Element {
  
  def program: Program
  
  def reference: Reference
  
  def name = reference.name
  
  def datasetType = reference.valueType

  def source: Tree
  
  def edges: List[Edge]
  
  def incomingEdges = edges.filter(e => e.direction == DirectionsEnum.TransformationToDataset).toList
  
  def outgoingEdges = edges.filter(e => e.direction == DirectionsEnum.DatasetToTransformation).toList
  
  def incomingTransformations = incomingEdges.map(e => e.transformation)
  
  def outgoingTransformations = outgoingEdges.map(e => e.transformation)
  
  def isInputDataset = incomingEdges.isEmpty
  
  def isOutputDataset = outgoingEdges.isEmpty
  
  def copy(id: Long = this.id, program: Program = this.program, reference: Reference = this.reference, source : Tree = this.source, edges : List[Edge] = this.edges) : Dataset
  
}