package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler

object SINDyAlgorithm {

  private def loadTimeSeriesData(inputPath: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "true")
      .csv(inputPath)
  }

  def applySINDyAlgorithm(inputPaths: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    // Load and merge time series data
    val combinedData = inputPaths.map(input => loadTimeSeriesData(input, spark))
      .reduce((dataset1, dataset2) => dataset1.union(dataset2))

    // Assuming you have a column 'value' containing your time series data, and 'time' as a time column
    val assembler = new VectorAssembler()
      .setInputCols(Array("time", "value"))
      .setOutputCol("features")

    val assembledData = assembler.transform(combinedData)



    combinedData.show()
  }
}
