import br.ufrn.dimap.forall.transmut.analyzer.CodeAnalyzer
//import br.ufrn.dimap.forall.transmut.analyzer.ProgramBuilder
import br.ufrn.dimap.forall.transmut.analyzer.TypesAnalyzer
import br.ufrn.dimap.forall.transmut.report._
import br.ufrn.dimap.forall.transmut.spark.analyzer.SparkRDDProgramBuilder



object TempMain {
  def main(args: Array[String]) {

    val programNames = List("sameHostProblem", "unionLogsProblem")
    val tree = CodeAnalyzer.getTreeFromPath("/Users/joaosouza/Repositories/scalameta-examples/src/main/scala/NasaApacheWebLogsAnalysis.scala")
    val refenceTypes = TypesAnalyzer.getReferenceMapFromPath("/Users/joaosouza/Repositories/scalameta-examples/target/scala-2.12/classes/META-INF/semanticdb/src/main/scala/NasaApacheWebLogsAnalysis.scala.semanticdb")
    val programSource = SparkRDDProgramBuilder.buildProgramSourceFromProgramNames(programNames, tree, refenceTypes)

    programSource.programs.foreach(p => ProgramReport.generateProgramHtmlReportFile(p, "./"))

  }
}