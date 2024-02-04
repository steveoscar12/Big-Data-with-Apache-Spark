package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }


  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val result =
      inputs.map(input => readData(input, spark))
        .map(table => {
          val columns = table.columns
          table.flatMap(row => for (i <- columns.indices) yield (columns(i), row.getString(i)))})
        .reduce((set1, set2) => set1 union set2)
        .groupByKey(t => t._2)
        .mapGroups((_, iterator) => iterator.map(_._1).toSet)
        .flatMap(Set => Set
          .map(currentAttribute => (currentAttribute, Set.filter(attribute => !attribute.equals(currentAttribute)))))
        .groupByKey(row => row._1)
        .mapGroups((key, iterator) => (key, iterator.map(row => row._2).reduce((set1, set2) => set1.intersect(set2))))
        .collect() // Here spark stops

    result.sortBy(tuple => tuple._1)
      .foreach(IND => if (IND._2.nonEmpty) println(IND._1 + " -> " + IND._2.mkString(",")))
  }
}
