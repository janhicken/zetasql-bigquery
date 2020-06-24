package io.github.janhicken.zetasql

import com.google.cloud.bigquery
import com.google.zetasql.SimpleCatalog

class BigQueryProject(projectId: String,
                      tables: Map[bigquery.DatasetId, Iterable[bigquery.Table]],
                      bigQueryOptions: bigquery.BigQueryOptions) extends SimpleCatalog(projectId) {
  tables
    .map { case (datasetId, tables) => new BigQueryDataset(datasetId, tables, bigQueryOptions) }
    .foreach(addSimpleCatalog)
}
