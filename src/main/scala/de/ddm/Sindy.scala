package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  private def readCSVData(inputPath: String, spark: SparkSession): Dataset[Row] = {
    spark.read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(inputPath)
  }

  def discoverInclusionDependencies(inputPaths: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val combinedData =
      inputPaths.map(input => readCSVData(input, spark))
        .reduce((dataset1, dataset2) => dataset1.union(dataset2))

    val allAttributes =
      combinedData.columns

    val flattenedData =
      combinedData.flatMap(row => for (i <- allAttributes.indices) yield (allAttributes(i), row.getString(i)))

    val attributeSets =
      flattenedData.groupByKey(t => t._2)
        .mapGroups((_, iterator) => iterator.map(_._1).toSet)

    val distinctAttributes =
      attributeSets.flatMap(attributeSet =>
        attributeSet.map(currentAttr =>
          (currentAttr, attributeSet.filter(attr => attr != currentAttr)))
      )

    val groupedAttributes =
      distinctAttributes.groupByKey(row => row._1)
        .reduceGroups((_, iterator) => iterator.map(row => row._2).reduce((set1, set2) => set1.intersect(set2)))

    val results =
      groupedAttributes.collect()

    results.sortBy(tuple => tuple._1)
      .foreach { case (attribute, indSet) =>
        if (indSet.nonEmpty) println(s"$attribute -> ${indSet.mkString(",")}")
      }
  }
}
