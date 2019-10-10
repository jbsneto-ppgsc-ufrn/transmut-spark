import br.ufrn.dimap.forall.transmut.analyzer.CodeAnalyzer
import br.ufrn.dimap.forall.transmut.analyzer.TypesAnalyzer
import br.ufrn.dimap.forall.transmut.report.ProgramReport
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder

object TempMain {
  def main(args: Array[String]) {

    val programNames = List("sameHostProblem", "unionLogsProblem")
    val tree = CodeAnalyzer.getTreeFromPath("./src/test/resources/NasaApacheWebLogsAnalysis.scala")
    val refenceTypes = TypesAnalyzer.getReferenceMapFromPath("./src/test/resources/NasaApacheWebLogsAnalysis.scala.semanticdb")

    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes)

    programSource.programs.foreach(p => ProgramReport.generateProgramHtmlReportFile(p, "./bin/"))

  }
}