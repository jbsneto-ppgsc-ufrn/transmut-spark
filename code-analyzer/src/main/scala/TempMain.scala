import br.ufrn.dimap.forall.transmut.analyzer.CodeParser
import br.ufrn.dimap.forall.transmut.analyzer.TypesAnalyzer
import br.ufrn.dimap.forall.transmut.report.ProgramReport
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder
import java.nio.file.Files
import java.nio.file.Paths

object TempMain {
  def main(args: Array[String]) {

    val sources = List("NasaApacheWebLogsAnalysis.scala", "SparkProgramTestCase1.scala")
    val programs = List("sameHostProblem", "unionLogsProblem", "program")
    val srcDir = Paths.get("./src/test/resources/src/")
    val semanticdbDir = Paths.get("./src/test/resources/meta/")

    val programSources = SparkRDDProgramBuilder.buildProgramSources(sources, programs, srcDir, semanticdbDir)

    programSources.foreach(programSource => programSource.programs.foreach(p => ProgramReport.generateProgramHtmlReportFile(p, "./bin/")))

  }
}