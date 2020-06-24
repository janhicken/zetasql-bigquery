package io.github.janhicken.zetasql

import com.google.cloud.bigquery
import com.google.cloud.bigquery.{StandardTableDefinition, TableDefinition}
import com.google.zetasql
import com.google.zetasql.ZetaSQLType.TypeKind._
import com.google.zetasql.{SimpleColumn, TypeFactory}

import scala.jdk.CollectionConverters._
import scala.util.Try

class BigQueryDataset(datasetId: bigquery.DatasetId, tables: Iterable[bigquery.Table], options: bigquery.BigQueryOptions)
  extends zetasql.SimpleCatalog(datasetId.getDataset) {

  import BigQueryDataset._

  tables
    .map(t => t.getTableId.getTable.toLowerCase -> buildTable(t))
    .toMap
    .foreach { case (name, table) => addSimpleTable(name, table) }

  Try(options.getService.listRoutines(datasetId).iterateAll().asScala)
    .getOrElse(Iterable.empty)
    .map(_.reload())
    .map(new BigQueryRoutine(_))
    .foreach(addFunction)

  override def getFullName: String = s"${datasetId.getProject}.${datasetId.getDataset}"
}

object BigQueryDataset {

  case class DatasetContent(tables: Iterable[bigquery.Table], routines: Iterable[bigquery.Routine])

  def buildTable(table: bigquery.Table): zetasql.SimpleTable = {
    val fullTableName = s"${table.getTableId.getProject}.${table.getTableId.getDataset}.${table.getTableId.getTable}"
    val tableDefinition = table.getDefinition[TableDefinition]

    val columns = tableDefinition.getSchema.getFields.asScala
      .map(f => new SimpleColumn(fullTableName, f.getName, convertType(f)))

    val pseudoColumns = tableDefinition match {
      case stdTableDefinition: StandardTableDefinition => Option(stdTableDefinition.getTimePartitioning)
        .iterator
        .flatMap { tp =>
          if (tp.getField == null) Seq(
            new SimpleColumn(fullTableName, "_PARTITIONDATE", TypeFactory.createSimpleType(TYPE_DATE)),
            new SimpleColumn(fullTableName, "_PARTITIONTIME", TypeFactory.createSimpleType(TYPE_TIMESTAMP)),
            new SimpleColumn(fullTableName, "_PARTITION_LOAD_TIME", TypeFactory.createSimpleType(TYPE_TIMESTAMP)))
          else Seq()
        }
      case _ => Seq()
    }

    new zetasql.SimpleTable(
      fullTableName,
      (columns ++ pseudoColumns).asJava
    )
  }
}
