package br.ufrn.dimap.forall.model

import DirectionsEnum._

case class Edge(override val id: Long) extends Element(id) {

  def this(id: Long, dataset: Dataset, transformation: Transformation, direction: DirectionsEnum) {
    this(id)
    _dataset = dataset
    _transformation = transformation
    _direction = direction
  }

  private var _dataset: Dataset = _
  private var _transformation: Transformation = _
  private var _direction: DirectionsEnum = _

  def dataset = _dataset
  def dataset_=(dataset: Dataset) {
    _dataset = dataset
  }

  def transformation = _transformation
  def transformation_=(transformation: Transformation) {
    _transformation = transformation
  }

  def direction = _direction
  def direction_=(direction: DirectionsEnum) {
    _direction = direction
  }
}

object Edge {

  def apply(id: Long, dataset: Dataset, transformation: Transformation, direction: DirectionsEnum) = new Edge(id, dataset, transformation, direction)

}