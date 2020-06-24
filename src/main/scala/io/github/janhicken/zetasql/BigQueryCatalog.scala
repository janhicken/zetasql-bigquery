package io.github.janhicken.zetasql

import com.google.cloud.bigquery.{Batcher, BigQueryOptions, DatasetId, TableId}
import com.google.zetasql.SimpleCatalog

import scala.jdk.CollectionConverters._
import scala.util.Success

class BigQueryCatalog(projectId: String,
                      includeDatasets: Option[Set[String]] = None,
                      additionalProjectIds: Set[String] = Set.empty,
                      options: BigQueryOptions = BigQueryOptions.getDefaultInstance)
  extends SimpleCatalog("ROOT") {

  import BigQueryCatalog._

  {
    val batcher = new Batcher(options)
    val projectIds: Set[String] = additionalProjectIds + projectId
    val datasets = includeDatasets
      .fold(batcher.listDatasets(projectIds))(incl => Success(incl.map(DatasetId.of(projectId, _))))

    datasets
      .flatMap(ds => batcher.listTables(ds).map(_ ++ ds.flatMap(createPseudoTables)))
      .flatMap(batcher.getTables)
      .get
      .groupBy(_.getTableId.getProject)
      .view
      .mapValues(_
        .groupBy(t => DatasetId.of(t.getTableId.getProject, t.getTableId.getDataset))
      )
      .map { case (projectId, tablesByDataset) =>
        new BigQueryProject(projectId, tablesByDataset, options)
      }
      .foreach(addSimpleCatalog)
  }

  /*
   * Register default project's datasets
   * Allows references like SELECT * FROM dataset.table
   */
  getCatalog(projectId).asInstanceOf[SimpleCatalog].getCatalogList.asScala
    .foreach(addSimpleCatalog)

  /*
   * Register default project's datasets' tables
   * Allows references like SELECT * FROM `dataset.table`
   */
  getCatalog(projectId).asInstanceOf[SimpleCatalog].getCatalogList.asScala
    .flatMap(_.getTableList.asScala)
    .foreach(t => addSimpleTable(t.getFullName.substring(t.getFullName.indexOf('.') + 1), t))

  /*
   * Register each table with its full name
   * Allows references like SELECT * FROM `project.dataset.table`
   */
  getCatalogList.asScala
    .flatMap(_.getCatalogList.asScala)
    .flatMap(_.getTableList.asScala)
    .foreach(addSimpleTable)

  // Register BigQuery's built-in functions
  addBuiltinFunctions(this)
}

object BigQueryCatalog {
  private def createPseudoTables(datasetId: DatasetId): Set[TableId] = Set("__TABLES__", "__TABLES_SUMMARY__")
    .map(TableId.of(datasetId.getProject, datasetId.getDataset, _))
}
