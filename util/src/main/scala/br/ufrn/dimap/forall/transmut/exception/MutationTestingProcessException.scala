package br.ufrn.dimap.forall.transmut.exception

sealed abstract class MutationTestingProcessException(message: String) extends Exception(message)

final case class ConfigurationException(message: String) extends MutationTestingProcessException(message)
final case class ProgramBuildException(message: String) extends MutationTestingProcessException(message)
final case class OriginalTestExecutionException(message: String) extends MutationTestingProcessException(message)