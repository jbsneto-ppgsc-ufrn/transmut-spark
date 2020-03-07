package br.ufrn.dimap.forall.transmut.util

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import java.nio.file.Files
import java.nio.file.Paths
import java.io.File

class IOFilesTestSuite extends FunSuite with BeforeAndAfter {
  
  val testBaseDir = Paths.get("./bin/", "io-tests")

  before {
    if (Files.exists(testBaseDir) && Files.isDirectory(testBaseDir)) {
      IOFiles.deleteFile(testBaseDir.toFile())
    }
  }
  
  test("Test Case 1 - Generate Directory"){
    val directory = new File("./bin/io-tests/test")
    assert(!directory.exists())
    IOFiles.generateDirectory(testBaseDir.toFile(), "test")
    assert(directory.exists())
  }
  
  test("Test Case 2 - Generate File and Read Content"){
    val file = new File("./bin/io-tests/test.txt")
    assert(!file.exists())
    val content = "Test File Content"
    IOFiles.generateFileWithContent(testBaseDir.toFile, "test.txt", content)
    assert(file.exists())
    val readContent = IOFiles.readContentFromFileName(testBaseDir.toFile, "test.txt")
    assert(readContent == content)
  }
  
  after {
    if (Files.exists(testBaseDir) && Files.isDirectory(testBaseDir)) {
      IOFiles.deleteFile(testBaseDir.toFile())
    }
  }
}