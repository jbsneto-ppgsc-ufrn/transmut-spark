package br.ufrn.dimap.forall.transmut.model

import scala.collection.mutable.ListBuffer
import scala.meta.Tree

trait Transformation extends Element {
  
  def program: Program
  
  def name : String
  
  def source : Tree
  
  def params: List[Tree]
  
  def edges : List[Edge]
  
  def incomingEdges = edges.filter(e => e.direction == DirectionsEnum.DatasetToTransformation).toList
  
  def outgoingEdges = edges.filter(e => e.direction == DirectionsEnum.TransformationToDataset).toList
  
  def incomingDatasets = incomingEdges.map(e => e.dataset)
  
  def outgoingDatasets = outgoingEdges.map(e => e.dataset)
  
  def inputTypes = incomingEdges.map(e => e.dataset.datasetType)
  
  def outputTypes = outgoingEdges.map(e => e.dataset.datasetType)
  
  def isLoadTransformation = outgoingEdges.isEmpty
  
  def copy(id : Long = this.id, program: Program = this.program, name : String = this.name, source : Tree = this.source, params : List[Tree] = this.params, edges : List[Edge] = this.edges) : Transformation
  
}