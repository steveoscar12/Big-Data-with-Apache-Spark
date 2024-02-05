package de.ddm

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Sindy {

  private def readData(input: String, spark: SparkSession): DataFrame = {
    spark.read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val tables = inputs.map(input => readData(input, spark))

    val unionDF = tables.reduce(_ unionByName _)

    val attributeSets = unionDF
      .flatMap(row => row.schema.fields.map(field => (field.name, row.getAs[String](field.name))))
      .groupByKey { case (_, value) => value }
      .mapGroups { case (_, iterator) => iterator.map { case (attribute, _) => attribute }.toSet }
      .persist()

    val indResults = attributeSets
      .flatMap { case (currentAttribute, attributeSet) =>
        attributeSet.map(attribute => (currentAttribute, attributeSet - attribute))
      }
      .groupByKey { case (currentAttribute, _) => currentAttribute }
      .mapGroups { case (currentAttribute, iterator) =>
        val intersectedSet = iterator.map { case (_, intersectSet) => intersectSet }.reduce(_ intersect _)
        (currentAttribute, intersectedSet)
      }
      .collect()

    indResults
      .sortBy { case (attribute, _) => attribute }
      .foreach { case (attribute, indSet) =>
        if (indSet.nonEmpty) println(s"$attribute -> ${indSet.mkString(",")}")
      }

    attributeSets.unpersist()
  }
}
