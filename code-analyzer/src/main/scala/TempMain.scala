import br.ufrn.dimap.forall.analyzer.TypesAnalyzer
import br.ufrn.dimap.forall.analyzer.CodeAnalyzer
import br.ufrn.dimap.forall.analyzer.ProgramBuilder
import br.ufrn.dimap.forall.report.ProgramReport

object TempMain {
  def main(args: Array[String]) {

    val programNames = List("sameHostProblem", "unionLogsProblem")
    val tree = CodeAnalyzer.getTreeFromPath("/Users/joaosouza/Repositories/scalameta-examples/src/main/scala/NasaApacheWebLogsAnalysis.scala")
    val refenceTypes = TypesAnalyzer.getReferenceMapFromPath("/Users/joaosouza/Repositories/scalameta-examples/target/scala-2.12/classes/META-INF/semanticdb/src/main/scala/NasaApacheWebLogsAnalysis.scala.semanticdb")
    val programSource = ProgramBuilder.buildProgramSourceFromReferenceNames(programNames, tree, refenceTypes)

    programSource.programs.foreach(p => ProgramReport.generateProgramHtmlReportFile(p, "./"))

  }
}