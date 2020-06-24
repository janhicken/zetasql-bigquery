package com.google.cloud.bigquery

import java.io.IOException
import java.util.logging.Logger

import com.google.api.client.googleapis.batch.json.JsonBatchCallback
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonError
import com.google.api.client.http.HttpHeaders
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services
import com.google.auth.http.HttpCredentialsAdapter

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class Batcher(options: BigQueryOptions) {

  import Batcher._

  private val client: services.bigquery.Bigquery = new services.bigquery.Bigquery.Builder(
    GoogleNetHttpTransport.newTrustedTransport(), JacksonFactory.getDefaultInstance,
    new HttpCredentialsAdapter(options.getScopedCredentials))
    .setApplicationName("ZetaSQL BigQuery Analyzer")
    .build()

  def listDatasets(projectIds: Set[String]): Try[Set[DatasetId]] = {
    Log.fine(s"Listing datasets for ${projectIds.size} project(s)")

    val requests = projectIds.map(projectId => client.datasets
      .list(projectId)
      .setMaxResults(MaxResults))

    batch(requests).map(_
      .flatMap(
        _.getDatasets.asScala
          .map(d => DatasetId.fromPb(d.getDatasetReference))
      )
    )
  }

  def listTables(datasetIds: Set[DatasetId]): Try[Set[TableId]] = {
    Log.fine(s"Listing tables for ${datasetIds.size} dataset(s)")
    val requests = datasetIds.map(datasetId => client.tables
      .list(datasetId.getProject, datasetId.getDataset)
      .setMaxResults(MaxResults))

    batch(requests).map(_
      .filter(_.getTotalItems > 0)
      .flatMap(_.getTables.asScala
        .map(t => TableId.fromPb(t.getTableReference))
      )
    )
  }

  def getTables(tableIds: Set[TableId]): Try[Set[Table]] = {
    Log.fine(s"Getting ${tableIds.size} table(s)")
    val requests = tableIds.map(tableId => client.tables
      .get(tableId.getProject, tableId.getDataset, tableId.getTable))

    batch(requests).map(_.map(Table.fromPb(options.getService, _)))
  }

  private def batch[Resp](requests: Iterable[services.bigquery.BigqueryRequest[Resp]]): Try[Set[Resp]] = {
    val batch = client.batch()
    val callback = new BatchCallbackAccumulator[Resp]

    requests
      .grouped(BatchSize)
      .foreach { group =>
        group.foreach(_.queue(batch, callback))
        batch.execute()
      }

    if (callback.errors.nonEmpty)
      Failure(new BigQueryException(new IOException(callback.errors.toString())))
    else
      Success(callback.responses)
  }
}

object Batcher {

  private val MaxResults: Long = 1000L
  private val BatchSize: Int = 1000

  private val Log: Logger = Logger.getLogger(classOf[Batcher].getName)

  private class BatchCallbackAccumulator[A] extends JsonBatchCallback[A] {
    private val responseBuilder = Set.newBuilder[A]
    private val errorsBuilder = Set.newBuilder[String]

    override def onFailure(err: GoogleJsonError, responseHeaders: HttpHeaders): Unit =
      errorsBuilder += err.getMessage

    override def onSuccess(a: A, responseHeaders: HttpHeaders): Unit =
      responseBuilder += a

    lazy val errors: Set[String] = errorsBuilder.result()

    lazy val responses: Set[A] = responseBuilder.result()
  }

}
