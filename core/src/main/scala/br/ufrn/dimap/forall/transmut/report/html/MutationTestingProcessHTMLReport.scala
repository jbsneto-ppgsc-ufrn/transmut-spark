package br.ufrn.dimap.forall.transmut.report.html

import br.ufrn.dimap.forall.transmut.report.metric.MutationTestingProcessMetrics
import br.ufrn.dimap.forall.transmut.report.metric.MetaMutantProgramSourceMetrics
import br.ufrn.dimap.forall.transmut.util.IOFiles
import java.io.PrintWriter
import java.io.File
import java.util.Locale
import java.text.NumberFormat
import br.ufrn.dimap.forall.transmut.report.metric.MetaMutantProgramMetrics
import br.ufrn.dimap.forall.transmut.report.metric.MutantProgramMetrics
import br.ufrn.dimap.forall.transmut.mutation.operator.MutationOperatorsEnum

object MutationTestingProcessHTMLReport {

  def generateProgramHtmlReportFile(directory: File, fileName: String, metrics: MutationTestingProcessMetrics) {
    val content = generateProgramHtmlReport(metrics)
    IOFiles.generateFileWithContent(directory, fileName, content)
  }

  def generateProgramHtmlReport(metrics: MutationTestingProcessMetrics) = {
    s"""<!doctype html>
       |<html lang="en">
       |<head>
       |<!-- Required meta tags -->
       |<meta charset="utf-8">
       |<meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
       |<!-- Bootstrap CSS -->
       |<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
       |<!-- SyntaxHighlighter -->
       |<script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/SyntaxHighlighter/3.0.83/scripts/shCore.js"></script>
       |<script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/SyntaxHighlighter/3.0.83/scripts/shBrushScala.js"></script>
       |<link href="https://cdnjs.cloudflare.com/ajax/libs/SyntaxHighlighter/3.0.83/styles/shCoreEclipse.css" rel="stylesheet" type="text/css" />
       |<link href="https://cdnjs.cloudflare.com/ajax/libs/SyntaxHighlighter/3.0.83/styles/shThemeEclipse.min.css" rel="stylesheet" type="text/css" />
       |<!-- Cytoscape -->
       |<script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/2.5.1/cytoscape.min.js"></script>
       |<!-- DataTable -->
       |<link href="https://cdn.datatables.net/1.10.20/css/dataTables.bootstrap4.min.css" rel="stylesheet" type="text/css" />
       |<style>
       |#cy {
       |  width: 1100px;
       |  height: 300px;
       |  background-color: white;
       |}
       |body {
       |  padding-top: 5rem;
       |}
       |.starter-template {
       |  padding: 3rem 1.5rem;
       |  text-align: center;
       |}
       |.section-title {
	     |  padding-top: 3.5rem;
       |}
       |</style>
       |<title>TRANSMUT-Spark Mutation Testing Report</title>
       |</head>
       |<body>
       |<nav class="navbar navbar-expand-md navbar-dark bg-dark fixed-top">
       |<a class="navbar-brand" href="#">TRANSMUT-Spark</a>
       |<div class="collapse navbar-collapse" id="navbarNav">
       |  <ul class="navbar-nav">
       |    <li class="nav-item">
       |      <a class="nav-link" href="#programSources">Program Sources</a>
       |    </li>
       |    <li class="nav-item">
       |      <a class="nav-link" href="#programs">Programs</a>
       |    </li>
       |    <li class="nav-item">
       |      <a class="nav-link" href="#mutants">Mutants</a>
       |    </li>
       |  </ul>
       |</div>
       |</nav>
       |<main role"main" class="container">  
       |<div class="starter-template">
       |  <h2><a href="index.html" class="text-dark">Mutation Testing Report</a></h2>
       |  <h5>Process Duration: ${metrics.processDuration.toSeconds} seconds</h5>
       |</div>    
       |<!-- Program Sources -->
       |<div class="row" id="programSources">
       |<div class="col">
       |<h3 class="section-title">Program Sources</h3>
       |<hr class="my-4">
       |${generateProgramSourcesHtmlTable(metrics)}
       |<hr class="my-4">
       |</div>
       |</div>
       |<!-- Programs -->
       |<div class="row" id="programs">
       |<div class="col">
       |<h3 class="section-title">Programs</h3>
       |<hr class="my-4">
       |${generateProgramsHtmlTable(metrics)}
       |<hr class="my-4">
       |</div>
       |</div>
       |<!-- Mutants -->
       |<div class="row" id="mutants">
       |<div class="col">
       |<h3 class="section-title">Mutants</h3>
       |<hr class="my-4">
       |${generateMutantsHtmlTable(metrics)}
       |<hr class="my-4">
       |</div>
       |</div>
       |<!-- Mutant Modals -->
       |${generateMutantsModalsHtml(metrics)}
       |</main>
       |<!-- Optional JavaScript -->
       |<!-- jQuery first, then Popper.js, then Bootstrap JS -->
       |<script src="https://code.jquery.com/jquery-3.2.1.slim.min.js" integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN" crossorigin="anonymous"></script>
       |<script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js" integrity="sha384-ApNbgh9B+Y1QKtv3Rn7W3mgPxhU9K/ScQsAP7hUibX39j7fakFPskvXusvfa0b4Q" crossorigin="anonymous"></script>
       |<script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>
       |<script type="text/javascript" src="https://cdn.datatables.net/1.10.20/js/jquery.dataTables.min.js"></script>
       |<script type="text/javascript" src="https://cdn.datatables.net/1.10.20/js/dataTables.bootstrap4.min.js"></script>
       |<script type="text/javascript">
       | SyntaxHighlighter.all()
       |</script>
       |<script >
       |$$(document).ready(function() {
       |  $$('table.display').DataTable( {
       |    fixedHeader: {
       |      header: true,
       |      footer: true
       |    },
       |    "lengthMenu": [[5, 10, 20, -1], [5, 10, 20, "All"]]
       |  } );
       |} );
       |</script>
       |</body>
       |</html>
    """.stripMargin
  }

  def generateProgramSourcesHtmlTable(metrics: MutationTestingProcessMetrics) = {
    val rowsString = metrics.metaMutantProgramSourcesMetrics.map(generateProgramSourcesHtmlRow).mkString("\n")
    val generalMutationScore = "%1.2f".formatLocal(Locale.US, metrics.totalMutationScore * 100) + "%"
    val mutationScoreStyle = if (metrics.totalMutationScore >= 0.8) "bg-success" else if (metrics.totalMutationScore >= 0.5) "bg-warning" else "bg-danger"
    s"""<table class="display table table-striped table-hover" id="programSourcesTable">
      |  <thead class="thead-dark">
      |    <tr>
      |      <th scope="col">ID</th>
      |      <th scope="col">Program Source</th>
      |      <th scope="col">Programs</th>
      |      <th scope="col">Mutants</th>
      |      <th scope="col">Killed</th>
      |      <th scope="col">Survived</th>
      |      <th scope="col">Equivalent</th>
      |      <th scope="col">Error</th>
      |      <th scope="col">Mutation Score</th>
      |      </tr>
      |  </thead>
      |  <tbody>
      |    ${rowsString}
      |  </tbody>
      |  <tfoot class="text-light bg-secondary font-weight-bold">
      |    <tr>
      |      <th scope="row">#</th>
      |      <td>Total</td>
      |      <td>${metrics.totalMetaMutantPrograms}</td>
      |      <td>${metrics.totalMutants}</td>
      |      <td>${metrics.totalKilledMutants}</td>
      |      <td>${metrics.totalSurvivedMutants}</td>
      |      <td>${metrics.totalEquivalentMutants}</td>
      |      <td>${metrics.totalErrorMutants}</td>
      |      <td>
      |        <div class="progress">
      |          <div class="progress-bar progress-bar-striped ${mutationScoreStyle}" role="progressbar" style="width: ${generalMutationScore}" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100"><span class="font-weight-bold text-dark">${generalMutationScore}</span></div>
      |        </div>
      |      </td>
      |    </tr>
      |  </tfoot>
      |</table>   
   """.stripMargin
  }

  def generateProgramSourcesHtmlRow(metric: MetaMutantProgramSourceMetrics) = {
    val mutationScore = "%1.2f".formatLocal(Locale.US, metric.mutationScore * 100) + "%"
    val mutationScoreStyle = if (metric.mutationScore >= 0.8) "bg-success" else if (metric.mutationScore >= 0.5) "bg-warning" else "bg-danger"
    s"""<tr>
       |  <th scope="row"><a href="ProgramSources/Program-Source-${metric.id}.html" class="text-dark">${metric.id}</a></th>
       |  <td><a href="ProgramSources/Program-Source-${metric.id}.html" class="text-dark">${metric.sourceName.replaceFirst(".scala", "")}</a></td>
       |  <td>${metric.totalPrograms}</td>
       |  <td>${metric.totalMutants}</td>
       |  <td>${metric.totalKilledMutants}</td>
       |  <td>${metric.totalSurvivedMutants}</td>
       |  <td>${metric.totalEquivalentMutants}</td>
       |  <td>${metric.totalErrorMutants}</td>
       |  <td>
       |    <div class="progress">
       |      <div class="progress-bar progress-bar-striped ${mutationScoreStyle}" role="progressbar" style="width: ${mutationScore}" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100"><span class="font-weight-bold text-dark">${mutationScore}</span></div>
       |    </div>
       | </td>
       |</tr>""".stripMargin
  }

  def generateProgramsHtmlTable(metrics: MutationTestingProcessMetrics) = {
    val rowsString = metrics.metaMutantProgramsMetrics.map(generateProgramsHtmlRow).mkString("\n")
    val generalMutationScore = "%1.2f".formatLocal(Locale.US, metrics.totalMutationScore * 100) + "%"
    val mutationScoreStyle = if (metrics.totalMutationScore >= 0.8) "bg-success" else if (metrics.totalMutationScore >= 0.5) "bg-warning" else "bg-danger"
    s"""<table class="display table table-striped table-hover" id="programsTable">
      |  <thead class="thead-dark">
      |    <tr>
      |      <th scope="col">ID</th>
      |      <th scope="col">Program</th>
      |      <th scope="col">Datasets</th>
      |      <th scope="col">Transformations</th>
      |      <th scope="col">Mutants</th>
      |      <th scope="col">Killed</th>
      |      <th scope="col">Survived</th>
      |      <th scope="col">Equivalent</th>
      |      <th scope="col">Error</th>
      |      <th scope="col">Mutation Score</th>
      |      </tr>
      |  </thead>
      |  <tbody>
      |    ${rowsString}
      |  </tbody>
      |  <tfoot class="text-light bg-secondary font-weight-bold">
      |    <tr>
      |      <th scope="row">#</th>
      |      <td>Total</td>
      |      <td>${metrics.totalDatasets}</td>
      |      <td>${metrics.totalTransformations}</td>
      |      <td>${metrics.totalMutants}</td>
      |      <td>${metrics.totalKilledMutants}</td>
      |      <td>${metrics.totalSurvivedMutants}</td>
      |      <td>${metrics.totalEquivalentMutants}</td>
      |      <td>${metrics.totalErrorMutants}</td>
      |      <td>
      |        <div class="progress">
      |          <div class="progress-bar progress-bar-striped ${mutationScoreStyle}" role="progressbar" style="width: ${generalMutationScore}" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100"><span class="font-weight-bold text-dark">${generalMutationScore}</span></div>
      |        </div>
      |      </td>
      |    </tr>
      |  </tfoot>
      |</table>   
   """.stripMargin
  }

  def generateProgramsHtmlRow(metric: MetaMutantProgramMetrics) = {
    val mutationScore = "%1.2f".formatLocal(Locale.US, metric.mutationScore * 100) + "%"
    val mutationScoreStyle = if (metric.mutationScore >= 0.8) "bg-success" else if (metric.mutationScore >= 0.5) "bg-warning" else "bg-danger"
    s"""<tr>
       |  <th scope="row"><a href="Programs/Program-${metric.id}.html" class="text-dark">${metric.id}</a></th>
       |  <td><a href="Programs/Program-${metric.id}.html" class="text-dark">${metric.name}</a></td>
       |  <td>${metric.totalDatasets}</td>
       |  <td>${metric.totalTransformations}</td>
       |  <td>${metric.totalMutants}</td>
       |  <td>${metric.totalKilledMutants}</td>
       |  <td>${metric.totalSurvivedMutants}</td>
       |  <td>${metric.totalEquivalentMutants}</td>
       |  <td>${metric.totalErrorMutants}</td>
       |  <td>
       |    <div class="progress">
       |      <div class="progress-bar progress-bar-striped ${mutationScoreStyle}" role="progressbar" style="width: ${mutationScore}" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100"><span class="font-weight-bold text-dark">${mutationScore}</span></div>
       |    </div>
       | </td>
       |</tr>""".stripMargin
  }
  
  def generateMutantsHtmlTable(metrics: MutationTestingProcessMetrics) = {
    val rowsString = metrics.mutantProgramsMetrics.map(generateMutantsHtmlRow).mkString("\n")
    val generalMutationScore = "%1.2f".formatLocal(Locale.US, metrics.totalMutationScore * 100) + "%"
    val mutationScoreStyle = if (metrics.totalMutationScore >= 0.8) "bg-success" else if (metrics.totalMutationScore >= 0.5) "bg-warning" else "bg-danger"
    s"""<table class="display table table-striped table-hover" id="programsTable">
      |  <thead class="thead-dark">
      |    <tr>
      |      <th scope="col">ID</th>
      |      <th scope="col">Program</th>
      |      <th scope="col">Mutation Operator</th>
      |      <th scope="col">Status</th>
      |      <th scope="col">Code</th>
      |     </tr>
      |  </thead>
      |  <tbody>
      |    ${rowsString}
      |  </tbody>
      |  <tfoot class="text-light bg-secondary font-weight-bold">
      |    <tr>
      |      <th scope="row" colspan="2">Mutation Score</th>
      |      <td colspan="3">
      |        <div class="progress">
      |          <div class="progress-bar progress-bar-striped ${mutationScoreStyle}" role="progressbar" style="width: ${generalMutationScore}" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100"><span class="font-weight-bold text-dark">${generalMutationScore}</span></div>
      |        </div>
      |      </td>
      |    </tr>
      |  </tfoot>
      |</table>   
   """.stripMargin
  }

  def generateMutantsHtmlRow(metric: MutantProgramMetrics) = {
    s"""<tr>
       |  <th scope="row"><a href="Mutants/Mutant-${metric.mutantId}.html" class="text-dark">${metric.mutantId}</a></th>
       |  <td><a href="Programs/Program-${metric.originalProgramId}.html" class="text-dark">${metric.name}</a></td>
       |  <td>${metric.mutationOperator}</td>
       |  <td>${metric.status}</td>
       |  <td><button type="button" class="btn btn-secondary" data-toggle="modal" data-target="#modalMutant${metric.mutantId}">Show</button></td>
       |</tr>""".stripMargin
  }
  
  def generateMutantsModalsHtml(metrics: MutationTestingProcessMetrics) = {
    metrics.mutantProgramsMetrics.map(generateMutantModalHtml).mkString("\n")
  }
  
  def generateMutantModalHtml(metric: MutantProgramMetrics) = {
    s"""<div class="modal fade" id="modalMutant${metric.mutantId}" tabindex="-1" role="dialog" aria-labelledby="modalMutantLabel${metric.mutantId}" aria-hidden="true">
       |<div class="modal-dialog modal-dialog-centered modal-lg" role="document">
       |<div class="modal-content">
       |<div class="modal-header">
       |<h5 class="modal-title" id="exampleModalLabel">Mutant ID: <a href="Mutants/Mutant-${metric.mutantId}.html" class="text-dark">${metric.mutantId}</a></h5>
       |<button type="button" class="close" data-dismiss="modal" aria-label="Close">
       |<span aria-hidden="true">&times;</span>
       |</button>
       |</div>
       |<div class="modal-body">
       |<h5>Mutation Operator: ${metric.mutationOperator}</h5>
       |<h5>Status: ${metric.status}</h5>
       |<h5>Code: </h5>
       |<pre class="brush: scala; toolbar: false;">
       |${metric.mutantCode}
       |</pre>
       |</div>
       |<div class="modal-footer">
       |<button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>
       |</div>
       |</div>
       |</div>
       |</div>""".stripMargin
  }

}