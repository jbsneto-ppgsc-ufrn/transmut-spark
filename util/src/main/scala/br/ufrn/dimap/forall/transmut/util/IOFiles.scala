package br.ufrn.dimap.forall.transmut.util

import java.io.File
import java.nio.file._
import java.io.PrintWriter

object IOFiles {

  def generateFileWithContent(directory: File, fileName: String, content: String) {
    if (!directory.exists())
      directory.mkdirs()
    val newFile = new File(directory, fileName)
    if (newFile.exists())
      newFile.delete() // delete to re-write
    writeContentToFile(newFile, content)
  }

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

  def deleteFile(d: File): Boolean = {
    if (d.exists()) {
      if (d.isFile()) {
        d.delete()
      } else if (d.isDirectory) {
        d.listFiles.filter(_.isFile).foreach(f => f.delete())
        d.listFiles.filter(_.isDirectory).foreach(dir => deleteFile(dir))
        d.delete()
      } else {
        false
      }
    } else false
  }

  def copyFile(fileToCopy: File, toDir: File): Boolean = {
    if (fileToCopy.exists() && fileToCopy.isFile()) {
      if (!toDir.exists())
        toDir.mkdirs()
      val file = Files.copy(fileToCopy.toPath(), fileWithSeparatorAnd(toDir, fileToCopy.getName).toPath(), StandardCopyOption.REPLACE_EXISTING)
      file.toFile.exists()
    } else {
      false
    }
  }

  def copyDirectory(fromDir: File, toDir: File): Boolean = {
    if (fromDir.exists && fromDir.isDirectory) {
      if (!toDir.exists())
        toDir.mkdirs()
      // copy files
      fromDir.listFiles.filter(_.isFile).foreach { f =>
        val fileToCopy = fileWithSeparatorAnd(toDir, f.getName)
        deleteFile(fileToCopy)
        Files.copy(f.toPath, fileToCopy.toPath, StandardCopyOption.REPLACE_EXISTING)
      }
      // copy folders and its contents
      fromDir.listFiles.filter(_.isDirectory).foreach(f => {
        val foldetToCopy = f.toPath
        val folderToCopyTo = fileWithSeparatorAnd(toDir, f.getName).toPath
        deleteFile(folderToCopyTo.toFile())
        Files.copy(foldetToCopy, folderToCopyTo, StandardCopyOption.REPLACE_EXISTING)
        copyDirectory(foldetToCopy.toFile, folderToCopyTo.toFile())
      })
      true
    } else
      false
  }

  // Represents file / component
  private def fileWithSeparatorAnd(file: File, component: String): File = if (component == ".") file else new File(file, component)

}