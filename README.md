# ZetaSQL Analyzer for BigQuery

`zetasql-bigquery` provides glue to use the [ZetaSQL](https://github.com/google/zetasql) framework with your BigQuery 
datasets.

## Usage

### Maven Dependency
```xml
<dependency>
    <groupId>io.github.janhicken</groupId>
    <artifactId>zetasql-bigquery</artifactId>
    <version>0.1</version>
</dependency>
``` 

### Example Usage

```scala
import com.google.zetasql.Analyzer
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement
import io.github.janhicken.zetasql.{BigQueryAnalyzerOptions, BigQueryCatalog}

object Test {
    def main(args: Array[String]): Unit = {
        val sql = "SELECT COUNT(*) FROM samples.shakespeare"
        val catalog = new BigQueryCatalog("bigquery-public-data", Some(Set("samples")))
        val stmt = Analyzer.analyzeStatement(sql, BigQueryAnalyzerOptions, catalog)
    }  
}
``` 

## Known issues

Currently, the following features are not supported yet:
 * [BigQuery ML](https://cloud.google.com/bigquery-ml/docs/)
 * [Scripting](https://cloud.google.com/bigquery/docs/reference/standard-sql/scripting)
