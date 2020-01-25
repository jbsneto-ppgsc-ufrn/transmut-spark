package br.ufrn.dimap.forall.transmut.config

import java.io.File

import pureconfig._
import pureconfig.generic.auto._

object ConfigReader {

  def readConfig(configFile: File): Config = {
    if (isConfigFileExists(configFile))
      ConfigSource.file(configFile).at("transmut").loadOrThrow[Config]
    else
      throw new Exception("Configuration file does not exists: " + configFile.getAbsolutePath)
  }

  private def isConfigFileExists(configFile: File) = configFile.exists() && configFile.isFile()

}