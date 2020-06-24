package io.github.janhicken

import com.google.cloud.bigquery
import com.google.cloud.bigquery.{LegacySQLTypeName, StandardSQLTypeName}
import com.google.zetasql
import com.google.zetasql.StructType.StructField
import com.google.zetasql.ZetaSQLType.TypeKind._
import com.google.zetasql.{AnalyzerOptions, LanguageOptions, TypeFactory}

import scala.jdk.CollectionConverters._

package object zetasql {

  val BigQueryAnalyzerOptions: AnalyzerOptions = {
    val languageOptions = new LanguageOptions
    languageOptions.enableMaximumLanguageFeatures()
    languageOptions.setSupportsAllStatementKinds()

    val analyzerOptions = new AnalyzerOptions
    analyzerOptions.setDefaultTimezone("UTC")
    analyzerOptions.setLanguageOptions(languageOptions)
    analyzerOptions.setRecordParseLocations(true)

    analyzerOptions
  }

  def addBuiltinFunctions(catalog: zetasql.SimpleCatalog): Unit = {
    val zetaSQLBuiltinFunctionOptions = new zetasql.ZetaSQLBuiltinFunctionOptions
    zetasql.ZetaSQLFunction.FunctionSignatureId.values
      .foreach(zetaSQLBuiltinFunctionOptions.includeFunctionSignatureId)
    catalog.addZetaSQLFunctions(zetaSQLBuiltinFunctionOptions)
  }

  def convertType(`type`: bigquery.StandardSQLDataType): zetasql.Type =
    bigquery.StandardSQLTypeName.valueOf(`type`.getTypeKind) match {
      case StandardSQLTypeName.ARRAY => TypeFactory.createArrayType(convertType(`type`.getArrayElementType))
      case StandardSQLTypeName.BOOL => TypeFactory.createSimpleType(TYPE_BOOL)
      case StandardSQLTypeName.BYTES => TypeFactory.createSimpleType(TYPE_BYTES)
      case StandardSQLTypeName.DATE => TypeFactory.createSimpleType(TYPE_DATE)
      case StandardSQLTypeName.DATETIME => TypeFactory.createSimpleType(TYPE_DATETIME)
      case StandardSQLTypeName.FLOAT64 => TypeFactory.createSimpleType(TYPE_FLOAT)
      case StandardSQLTypeName.GEOGRAPHY => TypeFactory.createSimpleType(TYPE_GEOGRAPHY)
      case StandardSQLTypeName.INT64 => TypeFactory.createSimpleType(TYPE_INT64)
      case StandardSQLTypeName.NUMERIC => TypeFactory.createSimpleType(TYPE_NUMERIC)
      case StandardSQLTypeName.STRING => TypeFactory.createSimpleType(TYPE_STRING)
      case StandardSQLTypeName.STRUCT => TypeFactory.createStructType(`type`.getStructType.getFields.asScala
        .map(f => new StructField(f.getName, convertType(f.getDataType)))
        .asJava)
      case StandardSQLTypeName.TIME => TypeFactory.createSimpleType(TYPE_TIME)
      case StandardSQLTypeName.TIMESTAMP => TypeFactory.createSimpleType(TYPE_TIMESTAMP)
    }

  def convertType(field: bigquery.Field): zetasql.Type = {
    val `type` = field.getType match {
      case LegacySQLTypeName.BOOLEAN => TypeFactory.createSimpleType(TYPE_BOOL)
      case LegacySQLTypeName.BYTES => TypeFactory.createSimpleType(TYPE_BYTES)
      case LegacySQLTypeName.DATE => TypeFactory.createSimpleType(TYPE_DATE)
      case LegacySQLTypeName.DATETIME => TypeFactory.createSimpleType(TYPE_DATETIME)
      case LegacySQLTypeName.FLOAT => TypeFactory.createSimpleType(TYPE_FLOAT)
      case LegacySQLTypeName.GEOGRAPHY => TypeFactory.createSimpleType(TYPE_GEOGRAPHY)
      case LegacySQLTypeName.INTEGER => TypeFactory.createSimpleType(TYPE_INT64)
      case LegacySQLTypeName.NUMERIC => TypeFactory.createSimpleType(TYPE_NUMERIC)
      case LegacySQLTypeName.STRING => TypeFactory.createSimpleType(TYPE_STRING)
      case LegacySQLTypeName.TIME => TypeFactory.createSimpleType(TYPE_TIME)
      case LegacySQLTypeName.TIMESTAMP => TypeFactory.createSimpleType(TYPE_TIMESTAMP)
      case LegacySQLTypeName.RECORD => TypeFactory.createStructType(field.getSubFields.asScala
        .map(f => new StructField(f.getName, convertType(f)))
        .asJava)
    }

    if (field.getMode == bigquery.Field.Mode.REPEATED) {
      TypeFactory.createArrayType(`type`)
    } else {
      `type`
    }
  }
}
