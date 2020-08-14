package br.ufrn.dimap.forall.transmut.report.html

import br.ufrn.dimap.forall.transmut.report.metric.MutantProgramMetrics
import br.ufrn.dimap.forall.transmut.util.IOFiles
import br.ufrn.dimap.forall.transmut.report.metric.MetaMutantProgramSourceMetrics
import br.ufrn.dimap.forall.transmut.report.metric.MetaMutantProgramMetrics
import java.util.Locale
import java.io.File
import br.ufrn.dimap.forall.transmut.model.Transformation
import br.ufrn.dimap.forall.transmut.model.Dataset
import br.ufrn.dimap.forall.transmut.model.Program
import br.ufrn.dimap.forall.transmut.model.Edge
import br.ufrn.dimap.forall.transmut.model.DirectionsEnum
import br.ufrn.dimap.forall.transmut.report.metric.MutationOperatorsMetrics

object ProgramHTMLReport {

  def generateProgramHtmlReportFile(directory: File, fileName: String, metrics: MetaMutantProgramMetrics) {
    val content = generateProgramHtmlReport(metrics)
    IOFiles.generateFileWithContent(directory, fileName, content)
  }

  def generateProgramHtmlReport(metrics: MetaMutantProgramMetrics) = {
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
       |        <a class="dropdown-item" href="#metrics">Metrics</a>
       |        <a class="dropdown-item" href="#code">Code</a>
       |        <a class="dropdown-item" href="#dag">DAG</a>
       |        <a class="dropdown-item" href="#datasets">Datasets</a>
       |        <a class="dropdown-item" href="#transformations">Transformations</a>
       |        <a class="dropdown-item" href="#mutants">Mutants</a>
       |        <a class="dropdown-item" href="#mutationOperators">Mutation Operators</a>
       |      </div>
       |    </li>
       |  </ul>
       |</div>
       |</nav>
       |<main role"main" class="container">  
       |<div class="starter-template">
       |  <h2><a href="../index.html" class="text-dark">Mutation Testing Report</a></h2>
       |  <h3><a href="../ProgramSources/Program-Source-${metrics.programSourceId}.html" class="text-dark">Program Source: ${metrics.programSourceName}</a></h3>
       |  <h4><a href="#" class="text-dark">Program ID: ${metrics.id}</a></h4>
       |  <h4><a href="#" class="text-dark">Program: ${metrics.name}</a></h4>
       |</div>    
       |<!-- Metrics -->
       |<div class="row" id="metrics">
       |<div class="col" >
       |<h3 class="section-title">Metrics</h3>
       |<hr class="my-4">
       |${generateTotalMetricsHtmlTable(metrics)}
       |<hr class="my-4">
       |</div>
       |</div>
       |<!-- Code -->
       |<div class="row" id="code">
       |<div class="col">
       |<h3 class="section-title">Code</h3>
       |<hr class="my-4">
       |<pre class="brush: scala; toolbar: false;">
       |${metrics.code}
       |</pre>
       |<hr class="my-4">
       |</div>
       |</div>
       |<!-- DAG -->
       |<div class="row" id="dag">
       |<div class="col" >
       |<h3 class="section-title">DAG</h3>
       |<hr class="my-4">
       |<figure class="figure">
       |<div id="cy"></div>
       |</figure>
       |<hr class="my-4">
       |</div> 
       |</div>
       |<!-- Datasets -->
       |<div class="row" id="datasets">
       |<div class="col" >
       |<h3 class="section-title">Datasets</h3>
       |<hr class="my-4">
       |${generateDatasetsHtmlTable(metrics)}
       |<hr class="my-4">
       |</div>
       |</div>
       |<!-- Transformations -->
       |<div class="row" id="transformations">
       |<div class="col" >
       |<h3 class="section-title" id="transformations">Transformations</h3>
       |<hr class="my-4">
       |${generateTransformationsHtmlTable(metrics)}
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
       |<!-- Mutation Operators -->
       |<div class="row" id="mutationOperators">
       |<div class="col">
       |<h3 class="section-title">Mutation Operators</h3>
       |<hr class="my-4">
       |${generateMutationOperatorsHtmlTable(metrics)}
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
       |$$(function () {
       |  $$('[data-toggle="tooltip"]').tooltip()
       |});
       |</script>
       |${generateProgramGraph(metrics)}
       |</body>
       |</html>
    """.stripMargin
  }

  def generateTotalMetricsHtmlTable(metrics: MetaMutantProgramMetrics) = {
    val mutationScoreBar = "%1.2f".formatLocal(Locale.US, metrics.mutationScore * 100) + "%"
    val mutationScore = "%1.2f".formatLocal(Locale.US, metrics.mutationScore)
    val mutationScoreStyle = if (metrics.mutationScore >= 0.8) "bg-success" else if (metrics.mutationScore >= 0.5) "bg-warning" else "bg-danger"
    s"""<table class="table table-striped table-hover">
       |<thead class="thead-dark"><tr><th scope="col">#</th><th scope="col">Total</th></tr></thead>
       |<tbody>
       |  <tr><th scope="row">Datasets</th><td>${metrics.totalDatasets}</td></tr>
       |  <tr><th scope="row">Transformations</th><td>${metrics.totalTransformations}</td></tr>
       |  <tr><th scope="row">Mutants</th><td>${metrics.totalMutants}</td></tr>
       |  <tr><th scope="row">Killed Mutants</th><td>${metrics.totalKilledMutants}</td></tr>
       |  <tr><th scope="row">Lived Mutants</th><td>${metrics.totalLivedMutants}</td></tr>
       |  <tr><th scope="row">Equivalent Mutants</th><td>${metrics.totalEquivalentMutants}</td></tr>
       |  <tr><th scope="row">Error Mutants</th><td>${metrics.totalErrorMutants}</td></tr>
       |  <tr><th scope="row">Mutation Score</th>
       |  <td>
       |    <div class="progress">
       |      <div class="progress-bar progress-bar-striped ${mutationScoreStyle}" role="progressbar" style="width: ${mutationScoreBar}" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100"><span class="font-weight-bold text-dark">${mutationScore}</span></div>
       |    </div>
       |  </td>
       |  </tr>
       | </tbody>
       | <tfoot class="text-light bg-secondary font-weight-bold">
       |    <tr>
       |    <th scope="row"></th>
       |    <td></td>
       |    </tr>
       |  </tfoot>
       |</table>
     """.stripMargin
  }

  def generateMutantsHtmlTable(metrics: MetaMutantProgramMetrics) = {
    val rowsString = metrics.mutantsMetrics.map(generateMutantsHtmlRow).mkString("\n")
    val generalMutationScoreBar = "%1.2f".formatLocal(Locale.US, metrics.mutationScore * 100) + "%"
    val generalMutationScore = "%1.2f".formatLocal(Locale.US, metrics.mutationScore)
    val mutationScoreStyle = if (metrics.mutationScore >= 0.8) "bg-success" else if (metrics.mutationScore >= 0.5) "bg-warning" else "bg-danger"
    s"""<table class="display table table-striped table-hover" id="mutantsTable">
      |  <thead class="thead-dark">
      |    <tr>
      |      <th scope="col">ID</th>
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
      |      <td colspan="2">
      |        <div class="progress">
      |          <div class="progress-bar progress-bar-striped ${mutationScoreStyle}" role="progressbar" style="width: ${generalMutationScoreBar}" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100"><span class="font-weight-bold text-dark">${generalMutationScore}</span></div>
      |        </div>
      |      </td>
      |    </tr>
      |  </tfoot>
      |</table>   
   """.stripMargin
  }

  def generateMutantsHtmlRow(metric: MutantProgramMetrics) = {
    s"""<tr>
       |  <th scope="row"><a href="../Mutants/Mutant-${metric.mutantId}.html" class="text-dark">${metric.mutantId}</a></th>
       |  <td><a href="#" class="text-dark" data-toggle="tooltip" data-placement="right" title="${metric.mutationOperatorDescription}">${metric.mutationOperatorName}</a></td>
       |  <td>${metric.status}</td>
       |  <td><button type="button" class="btn btn-secondary" data-toggle="modal" data-target="#modalMutant${metric.mutantId}">Show</button></td>
       |</tr>""".stripMargin
  }

  def generateMutantsModalsHtml(metrics: MetaMutantProgramMetrics) = {
    metrics.mutantsMetrics.map(m => MutantHTMLReport.generateMutantModalHtml(m, false)).mkString("\n")
  }

  def generateTransformationsHtmlTable(metrics: MetaMutantProgramMetrics) = {
    val program: Program = metrics.originalProgram
    val rowsString = program.transformations.map(generateTransformationHtmlTableRow).mkString("\n")
    s"""<table class="table table-striped table-hover">
       |  <thead class="thead-dark">
       |    <tr>
       |      <th scope="col">ID</th>
       |      <th scope="col">Name</th>
       |      <th scope="col">Input Type</th>
       |      <th scope="col">Output Type</th>
       |   </tr>
       |  </thead>
       |  <tbody>
       |  ${rowsString}
       |  </tbody>
       |  <tfoot class="text-light bg-secondary font-weight-bold">
       |    <tr>
       |    <th scope="row" colspan="3">Total Transformations</th>
       |    <td>${metrics.totalTransformations}</td>
       |    </tr>
       |  </tfoot>
       |</table>      
    """.stripMargin
  }

  def generateTransformationHtmlTableRow(transformation: Transformation) = {
    val inputTypes = transformation.inputTypes.map(t => t.simplifiedName).mkString(", ")
    val outputTypes = transformation.outputTypes.map(t => t.simplifiedName).mkString(", ")
    s"""<tr>
      |  <th scope="row">${transformation.id}</th>
      |  <td>${transformation.name}</td>
      |  <td>${inputTypes}</td>
      |  <td>${outputTypes}</td>
      |</tr>""".stripMargin
  }

  def generateDatasetsHtmlTable(metrics: MetaMutantProgramMetrics) = {
    val program: Program = metrics.originalProgram
    val rowsString = program.datasets.map(generateDatasetHtmlTableRow).mkString("\n")
    s"""<table class="table table-striped table-hover">
       |  <thead class="thead-dark">
       |    <tr>
       |      <th scope="col">ID</th>
       |      <th scope="col">Reference</th>
       |      <th scope="col">Type</th>
       |   </tr>
       |  </thead>
       |  <tbody>
       |  ${rowsString}
       |  </tbody>
       |  <tfoot class="text-light bg-secondary font-weight-bold">
       |    <tr>
       |    <th scope="row" colspan="2">Total Datasets</th>
       |    <td>${metrics.totalDatasets}</td>
       |    </tr>
       |  </tfoot>
       |</table>      
    """.stripMargin
  }

  def generateDatasetHtmlTableRow(dataset: Dataset) = {
    s"""<tr>
      |  <th scope="row">${dataset.id}</th>
      |  <td>${dataset.name}</td>
      |  <td>${dataset.datasetType.simplifiedName}</td>
      |</tr>""".stripMargin
  }

  def generateMutationOperatorsHtmlTable(metrics: MetaMutantProgramMetrics) = {
    val mutationOperatorsMetrics = metrics.mutationOperatorsMetrics
    val rowsString = metrics.mutationOperatorsMetrics.totalMutantsPerOperator.keys.map(k => generateMutationOperatorHtmlRow(k, mutationOperatorsMetrics)).filter(s => !s.isEmpty()).mkString("\n")
    s"""<table class="display table table-striped table-hover" id="programSourcesTable">
      |  <thead class="thead-dark">
      |    <tr>
      |      <th scope="col">Mutation Operator</th>
      |      <th scope="col">Mutants</th>
      |      <th scope="col">Killed</th>
      |      <th scope="col">Lived</th>
      |      <th scope="col">Equivalent</th>
      |      <th scope="col">Error</th>
      |      </tr>
      |  </thead>
      |  <tbody>
      |    ${rowsString}
      |  </tbody>
      |  <tfoot class="text-light bg-secondary font-weight-bold">
      |    <tr>
      |      <th scope="row">Total</th>
      |      <td>${metrics.totalMutants}</td>
      |      <td>${metrics.totalKilledMutants}</td>
      |      <td>${metrics.totalLivedMutants}</td>
      |      <td>${metrics.totalEquivalentMutants}</td>
      |      <td>${metrics.totalErrorMutants}</td>
      |    </tr>
      |  </tfoot>
      |</table>   
   """.stripMargin
  }

  def generateMutationOperatorHtmlRow(mutationOperator: String, mutationOperatorsMetrics: MutationOperatorsMetrics) = {
    val totalMutants = mutationOperatorsMetrics.totalMutantsPerOperator.get(mutationOperator).getOrElse(0)
    val totalKilledMutants = mutationOperatorsMetrics.totalKilledMutantsPerOperator.get(mutationOperator).getOrElse(0)
    val totalLivedMutants = mutationOperatorsMetrics.totalLivedMutantsPerOperator.get(mutationOperator).getOrElse(0)
    val totalEquivalentMutants = mutationOperatorsMetrics.totalEquivalentMutantsPerOperator.get(mutationOperator).getOrElse(0)
    val totalErrorMutants = mutationOperatorsMetrics.totalErrorMutantsPerOperator.get(mutationOperator).getOrElse(0)
    val mutationOperatorDescription = mutationOperatorsMetrics.descriptionPerOperator.getOrElse(mutationOperator, "")
    if (totalMutants > 0) {
      s"""<tr>
       |  <th scope="row"><a href="#" class="text-dark" data-toggle="tooltip" data-placement="right" title="${mutationOperatorDescription}">${mutationOperator}</a></th>
       |  <td>${totalMutants}</td>
       |  <td>${totalKilledMutants}</td>
       |  <td>${totalLivedMutants}</td>
       |  <td>${totalEquivalentMutants}</td>
       |  <td>${totalErrorMutants}</td>
       |</tr>""".stripMargin
    } else ""
  }

  def generateProgramGraph(metrics: MetaMutantProgramMetrics) = {
    val program: Program = metrics.originalProgram
    val definedDatasets = scala.collection.mutable.Set[Dataset]()
    val definedTransformations = scala.collection.mutable.Set[Transformation]()

    val nodes = scala.collection.mutable.ListBuffer.empty[String]

    program.edges.foreach { edge =>
      if (!definedDatasets.contains(edge.dataset)) {
        definedDatasets += edge.dataset
        nodes += generateDatasetNode(edge.dataset)
      }
      if (!definedTransformations.contains(edge.transformation)) {
        definedTransformations += edge.transformation
        nodes += generateTransformationNode(edge.transformation)
      }
      nodes += generateEdgeNode(edge)
    }

    val nodesString = nodes.mkString(", \n")

    s"""
    |<script >
    |  var cy = cytoscape({
    |    container: document.getElementById('cy'), // container to render in
    |    elements: [ // list of graph elements to start with
    |      $nodesString
    |    ],
    |   style: [ // the stylesheet for the graph
    |   {
    |     selector: 'node',
    |     style: {
    |       'label': 'data(name)'
    |     }
    |   },
    |   {
    |     selector: 'edge',
    |     style: {
    |     'width': 3,
    |     'line-color': '#ccc',
    |     'target-arrow-color': '#ccc',
    |     'target-arrow-shape': 'triangle',
    |     }
    |   },
    |   {
    |      selector: '[transformation]',
    |      style: {
    |        'background-color': 'black',
    |        'shape': 'rectangle'
    |       }
    |   },
    |   {
    |      selector: '[dataset]',
    |      style: {
    |        'background-color': 'gray',
    |        'shape': 'ellipse'
    |      }
    |   },
    |   {
    |      selector: '[inputDataset]',
    |      style: {
    |        'background-color': 'green',
    |        'shape': 'ellipse'
    |       }
    |   },
    |   {
    |     selector: '[outputDataset]',
    |     style: {
    |       'background-color': 'red',
    |       'shape': 'ellipse'
    |      }
    |   },
    |   {
    |      selector: '[loadTransformation]',
    |      style: {
    |        'background-color': 'red',
    |        'shape': 'rectangle'
    |       }
    |   }
    |  ],
    |  layout: {
    |    name: 'grid',
    |    rows: 1
    |  }
    |});
    |</script>
    """.stripMargin
  }

  def generateDatasetNode(dataset: Dataset) =
    s"""{ // dataset node ${dataset.id}
    |  data: { id: 'dataset_${dataset.id}' , name : '${dataset.name}' , dataset : 'true' ${if (dataset.isInputDataset) ", inputDataset: 'true'" else ""} ${if (dataset.isOutputDataset) ", outputDataset: 'true'" else ""} }
    |}""".stripMargin

  def generateTransformationNode(transformation: Transformation) =
    s"""{ // transformation node ${transformation.id}
    |  data: { id: 'transformation_${transformation.id}' , name : '${transformation.name}' , transformation : 'true' ${if (transformation.isLoadTransformation) ", loadTransformation: 'true'" else ""} }
    |}""".stripMargin

  def generateEdgeNode(edge: Edge) =
    s"""{ // edge node ${edge.id}
    |  data: { id: 'edge_${edge.id}' ${if (edge.direction == DirectionsEnum.DatasetToTransformation) s", source: 'dataset_${edge.dataset.id}', target: 'transformation_${edge.transformation.id}'" else ""} ${if (edge.direction == DirectionsEnum.TransformationToDataset) s", target: 'dataset_${edge.dataset.id}', source: 'transformation_${edge.transformation.id}'" else ""} }
    |}""".stripMargin

}