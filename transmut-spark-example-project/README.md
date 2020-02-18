# TRANSMUT-Spark Example Project

Project with examples of Spark programs to run the TRANSMUT-Spark plugin.

## Sources, Programs and Tests
* `AggregationQuery.scala`
	* Programs: `aggregation`
	* Test Class: `example.AggregationQueryTest`
* `DistinctUserVisitsPerPage.scala`
	* Programs: `distinctUserVisitsPerPage`
	* Test Class: `example.DistinctUserVisitsPerPageTest`
* `JoinQuery.scala`
	* Programs: `join`
	* Test Class: `example.JoinQueryTest`
* `MoviesRatingsAverage.scala`
	* Programs: `moviesRatingsAverage`
	* Test Class: `example.MoviesRatingsAverageTest`
* `MoviesRecommendation.scala`
	* Programs: `moviesSimilaritiesTable`, `topNMoviesRecommendation`
	* Test Class: `example.MoviesRecommendationTest`
* `NasaApacheWebLogsAnalysis.scala`
	* Programs: `sameHostProblem`, `unionLogsProblem`
	* Test Class: `example.NasaApacheWebLogsAnalysisTest`
* `NGramsCount.scala`
	* Programs: `countNGrams`
	* Test Class: `example.NGramsCountTest`
* `ScanQuery.scala`
	* Programs: `scan`
	* Test Class: `example.ScanQueryTest`
* `WordCount.scala`
	* Programs: `wordCount`
	* Test Class: `example.WordCountTest`


## Running

Put in the [transmut.conf](transmut.conf) configuration file the list of sources, programs and test classes to execute in the process than execute the command `sbt transmut` in the terminal inside this project.

