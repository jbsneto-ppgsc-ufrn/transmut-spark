package br.ufrn.dimap.forall.transmut.mutation.runner

abstract class TestResult[T](val element: T)
case class TestSuccess[T](override val element: T) extends TestResult(element)
case class TestFailed[T](override val element: T) extends TestResult(element)
case class TestError[T](override val element: T) extends TestResult(element)