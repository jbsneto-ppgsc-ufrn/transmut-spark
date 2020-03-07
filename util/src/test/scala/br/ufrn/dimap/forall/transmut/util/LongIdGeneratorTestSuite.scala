package br.ufrn.dimap.forall.transmut.util

import org.scalatest.FunSuite

class LongIdGeneratorTestSuite extends FunSuite {
  
  test("Test Case 1 - Default Generator"){
    
    val generator = LongIdGenerator.generator
    
    assert(generator.getId == 1)
    assert(generator.getId == 2)
    assert(generator.getId == 3)
    
  }
  
  test("Test Case 2 - Generator Starts With"){
    
    val generator = LongIdGenerator.generator(23)
    
    assert(generator.getId == 23)
    assert(generator.getId == 24)
    assert(generator.getId == 25)
    
  }
  
}