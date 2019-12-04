package br.ufrn.dimap.forall.transmut.model

import DirectionsEnum._

trait Edge extends Element {

  def dataset: Dataset

  def transformation: Transformation

  def direction: DirectionsEnum
  
  def copy(id: Long = this.id, dataset: Dataset = this.dataset, transformation: Transformation = this.transformation, direction : DirectionsEnum = this.direction) : Edge

}