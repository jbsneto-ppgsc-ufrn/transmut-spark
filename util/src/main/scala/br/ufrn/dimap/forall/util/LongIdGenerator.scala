package br.ufrn.dimap.forall.util

class LongIdGenerator extends IdGenerator[Long] {
  
  private var currentId: Long = 0L
  
  private def this(startsWith : Long) {
    this()
    currentId = startsWith
  }
  
  def getId = {
    val id = currentId
    currentId += 1L
    id
  }
  
}

object LongIdGenerator {
  def generator = new LongIdGenerator
  def generator(startsWith : Long) = new LongIdGenerator(startsWith)
}