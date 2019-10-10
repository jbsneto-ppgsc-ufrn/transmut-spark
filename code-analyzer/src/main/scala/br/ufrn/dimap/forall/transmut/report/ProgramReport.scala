package br.ufrn.dimap.forall.transmut.report

import br.ufrn.dimap.forall.transmut.model._

import scala.collection.mutable.Set

import java.io._

object ProgramReport {
  
  def generateProgramHtmlReportFile(program: Program, directory: String) {
    val file = new PrintWriter(new File(directory + program.name + ".html"))
    file.write(generateProgramHtmlReport(program))
    file.close()
    println("Program " + directory + program.name + ".html created!") 
  }
  
  def generateProgramHtmlReport(program: Program) = {
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
       |<link href="https://cdnjs.cloudflare.com/ajax/libs/SyntaxHighlighter/3.0.83/styles/shCoreEclipse.css" rel="stylesheet" typ
       |<link href="https://cdnjs.cloudflare.com/ajax/libs/SyntaxHighlighter/3.0.83/styles/shThemeEclipse.min.css" rel="stylesheet" type="text/css" />
       |<!-- Cytoscape -->
       |<script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/2.5.1/cytoscape.min.js"></script>
       |<style>
       |#cy {
       |  width: 1100px;
       |  height: 300px;
       |  background-color: white;
       |}
       |</style>
       |<title>Program Report: ${program.name}</title>
       |</head>
       |<body>
       |<div class="container">
       |<div class="row">
       |<div class="col">
       |<nav class="navbar navbar-dark bg-dark">
       |<span class="navbar-brand mb-0 h1">Program Report: ${program.name}</span>
       |</nav>
       |</div>
       |</div>    
       |<!-- Code Place -->
       |<div class="row">
       |<div class="col" >
       |<h3>Code:</h3>
       |<pre class="brush: scala">
       |${program.tree.syntax}
       |</pre>
       |</div>
       |</div>
       |<!-- Graph Place -->
       |<div class="row">
       |<div class="col" >
       |<figure class="figure">
       |<h3>Program Dataflow:</h3>
       |<div id="cy"></div>
       |</figure>
       |</div>
       |</div>
       |<!-- Datasets Place -->
       |<div class="row">
       |<div class="col" >
       |<h3>Datasets:</h3>
       |${generateDatasetsHtmlTable(program)}
       |</div>
       |</div>
       |<div class="row">
       |<div class="col" >
       |<h3>Transformations:</h3>
       |${generateTransformationsHtmlTable(program)}
       |</div>
       |</div>
       |</div>
       |<!-- Optional JavaScript -->
       |<!-- jQuery first, then Popper.js, then Bootstrap JS -->
       |<script src="https://code.jquery.com/jquery-3.2.1.slim.min.js" integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN" crossorigin="anonymous"></script>
       |<script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js" integrity="sha384-ApNbgh9B+Y1QKtv3Rn7W3mgPxhU9K/ScQsAP7hUibX39j7fakFPskvXusvfa0b4Q" crossorigin="anonymous"></script>
       |<script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>
       |<script type="text/javascript">
       | SyntaxHighlighter.all()
       |</script>
       |${generateProgramGraph(program)}
       |</body>
       |</html>
    """.stripMargin
  }
  
  def generateTransformationsHtmlTable(program: Program) = {
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
  
  def generateDatasetsHtmlTable(program: Program) = {
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

  def generateProgramGraph(program: Program) = {

    val definedDatasets: Set[Dataset] = Set()
    val definedTransformations: Set[Transformation] = Set()

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
    |  data: { id: 'transformation_${transformation.id}' , name : '${transformation.name}' , transformation : 'true' ${if(transformation.isLoadTransformation) ", loadTransformation: 'true'" else ""} }
    |}""".stripMargin

  def generateEdgeNode(edge: Edge) =
    s"""{ // edge node ${edge.id}
    |  data: { id: 'edge_${edge.id}' ${if (edge.direction == DirectionsEnum.DatasetToTransformation) s", source: 'dataset_${edge.dataset.id}', target: 'transformation_${edge.transformation.id}'" else ""} ${if (edge.direction == DirectionsEnum.TransformationToDataset) s", target: 'dataset_${edge.dataset.id}', source: 'transformation_${edge.transformation.id}'" else ""} }
    |}""".stripMargin

}