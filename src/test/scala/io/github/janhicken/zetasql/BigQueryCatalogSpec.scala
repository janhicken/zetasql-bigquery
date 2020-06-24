package io.github.janhicken.zetasql

import com.google.zetasql.Analyzer
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

class BigQueryCatalogSpec extends AnyFeatureSpec with Matchers {

  private val catalog = new BigQueryCatalog("bigquery-public-data", Some(Set("samples", "persistent_udfs")))

  Feature("Table Reference") {
    Scenario("Unescaped dataset.table") {
      val sql = "SELECT COUNT(*) FROM samples.shakespeare"
      Analyzer.analyzeStatement(sql, BigQueryAnalyzerOptions, catalog)
    }

    Scenario("Escaped `dataset.table`") {
      val sql = "SELECT COUNT(*) FROM `samples.shakespeare`"
      Analyzer.analyzeStatement(sql, BigQueryAnalyzerOptions, catalog)
    }

    Scenario("Escaped `project.dataset.table`") {
      val sql = "SELECT COUNT(*) FROM `bigquery-public-data.samples.shakespeare`"
      Analyzer.analyzeStatement(sql, BigQueryAnalyzerOptions, catalog)
    }
  }

  Feature("Persistent UDFs") {
    Scenario("persistent_udfs.knots_to_mph") {
      val sql = "SELECT persistent_udfs.knots_to_mph(1.0)"
      Analyzer.analyzeStatement(sql, BigQueryAnalyzerOptions, catalog)
    }
  }
}
