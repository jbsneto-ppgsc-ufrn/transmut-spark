package br.ufrn.dimap.forall.transmut.util

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.io.PrintWriter

object IOFiles {

  def writeContentToFile(file: File, content: String) {
    val writer = new PrintWriter(file)
    writer.write(content)
    writer.close()
  }

  def getListOfFiles(d: File): List[File] = {
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList ++ d.listFiles.filter(_.isDirectory).flatMap(dir => getListOfFiles(dir))
    } else {
      List[File]()
    }
  }

  def createTempDirWithCopyFilesFromDir(dirToCopy: File, targetDir: File, prefix: String = ""): java.io.File = {
    val tempDir = Files.createTempDirectory(targetDir.toPath, prefix + dirToCopy.getName).toFile
    copyDirectory(dirToCopy, tempDir)
    tempDir
  }

  def copyDirectory(fromDir: File, toDir: File): Boolean = {
    if (fromDir.exists && fromDir.isDirectory) {
      if (!toDir.exists())
        toDir.mkdirs()
      // copy files
      fromDir.listFiles.filter(_.isFile).foreach(f => Files.copy(f.toPath, fileWithSeparatorAnd(toDir, f.getName).toPath))
      // copy folders and its contents
      fromDir.listFiles.filter(_.isDirectory).foreach(f => {
        val foldetToCopy = f.toPath
        val folderToCopyTo = fileWithSeparatorAnd(toDir, f.getName).toPath
        Files.copy(foldetToCopy, folderToCopyTo)
        copyDirectory(foldetToCopy.toFile, folderToCopyTo.toFile())
      })
      true
    } else
      false
  }

  // Represents file / component
  private def fileWithSeparatorAnd(file: File, component: String): File = if (component == ".") file else new File(file, component)

}