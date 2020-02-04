package br.ufrn.dimap.forall.transmut.report.html

import br.ufrn.dimap.forall.transmut.report.metric.MutantProgramMetrics
import br.ufrn.dimap.forall.transmut.util.IOFiles
import java.io.File

object MutantHTMLReport {
  
  def generateMutantHtmlReportFile(directory: File, fileName: String, metrics: MutantProgramMetrics) {
    val content = generateMutantHtmlReport(metrics)
    IOFiles.generateFileWithContent(directory, fileName, content)
  }

  def generateMutantHtmlReport(metrics: MutantProgramMetrics) = {
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
       |<a class="navbar-brand" href="../index.html">TRANSMUT-Spark</a>
       |<div class="collapse navbar-collapse" id="navbarNav">
       |  <ul class="navbar-nav mr-auto">
       |    <li class="nav-item dropdown">
       |      <a class="nav-link dropdown-toggle" href="#" id="navbarDropdown" role="button" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">Section</a>
       |      <div class="dropdown-menu" aria-labelledby="navbarDropdown">
       |        <a class="dropdown-item" href="#information">Information</a>
       |        <a class="dropdown-item" href="#originalCode">Original Code</a>
       |        <a class="dropdown-item" href="#mutantCode">Mutant Code</a>
       |      </div>
       |    </li>
       |  </ul>
       |</div>
       |</nav>
       |<main role"main" class="container">  
       |<div class="starter-template">
       |  <h2><a href="../index.html" class="text-dark">Mutation Testing Report</a></h2>
       |  <h3><a href="../ProgramSources/Program-Source-${metrics.originalProgramSourceId}.html" class="text-dark">Program Source: ${metrics.originalProgramSourceName}</a></h3>
       |  <h4><a href="../Programs/Program-${metrics.originalProgramId}.html" class="text-dark">Program: ${metrics.originalProgramName}</a></h4>
       |  <h4><a href="#" class="text-dark">Mutant ID: ${metrics.mutantId}</a></h4>
       |</div> 
       |<div class="row" id="information">
       |<div class="col" >
       |<h3 class="section-title">Information</h3>
       |<hr class="my-4">
       |  <h4><a href="#" class="text-dark">Mutant ID: ${metrics.mutantId}</a></h4>
       |  <h4>Mutation Operator: <a href="#" class="text-dark" data-toggle="tooltip" data-placement="right" title="${metrics.mutationOperatorDescription}">${metrics.mutationOperatorName}</a></h4>
       |  <h4>Status: ${metrics.status}</h4>
       |<hr class="my-4">
       |</div>
       |</div>
       |<!-- Original Code -->
       |<div class="row" id="originalCode">
       |<div class="col">
       |<h3 class="section-title">Original Code</h3>
       |<hr class="my-4">
       |<pre class="brush: scala; toolbar: false;">
       |${metrics.originalCode}
       |</pre>
       |<hr class="my-4">
       |</div>
       |</div>
       |<!-- Mutant Code -->
       |<div class="row" id="mutantCode">
       |<div class="col">
       |<h3 class="section-title">Mutant Code</h3>
       |<hr class="my-4">
       |<pre class="brush: scala; toolbar: false;">
       |${metrics.mutantCode}
       |</pre>
       |<hr class="my-4">
       |</div>
       |</div>
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
       |$$(function () {
       |  $$('[data-toggle="tooltip"]').tooltip()
       |});
       |</script>
       |</body>
       |</html>
    """.stripMargin
  }
}