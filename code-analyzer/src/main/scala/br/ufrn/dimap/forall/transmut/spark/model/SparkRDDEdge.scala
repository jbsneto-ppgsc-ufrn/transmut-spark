package br.ufrn.dimap.forall.transmut.spark.model

import br.ufrn.dimap.forall.transmut.model._
import br.ufrn.dimap.forall.transmut.model.DirectionsEnum._

case class SparkRDDEdge(override val id: Long) extends Edge {

  def this(id: Long, dataset: SparkRDD, transformation: SparkRDDTransformation, direct: DirectionsEnum) {
    this(id)
    _dataset = dataset
    _transformation = transformation
    _direction = direct
  }

  private var _direction: DirectionsEnum = _
  private var _dataset: SparkRDD = _
  private var _transformation: SparkRDDTransformation = _
  
  override def direction: DirectionsEnum = _direction
  
  def direction_=(direct: DirectionsEnum) {
    _direction = direct
  }

  override def dataset = _dataset
  
  def dataset_=(dataset: SparkRDD) {
    _dataset = dataset
  }

  override def transformation = _transformation
  
  def transformation_=(transformation: SparkRDDTransformation) {
    _transformation = transformation
  }

}

object SparkRDDEdge {
  def apply(id: Long, dataset: SparkRDD, transformation: SparkRDDTransformation, direct: DirectionsEnum) = new SparkRDDEdge(id, dataset, transformation, direct)
}