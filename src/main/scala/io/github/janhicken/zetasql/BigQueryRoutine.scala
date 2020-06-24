package io.github.janhicken.zetasql

import java.util

import com.google.cloud.bigquery
import com.google.common.collect.ImmutableList
import com.google.zetasql
import com.google.zetasql.{Analyzer, FunctionArgumentType, FunctionSignature, TypeFactory}

import scala.jdk.CollectionConverters._

class BigQueryRoutine(routine: bigquery.Routine) extends zetasql.Function(
  Seq(routine.getRoutineId.getProject, routine.getRoutineId.getDataset, routine.getRoutineId.getRoutine).asJava,
  BigQueryRoutine.FunctionGroupName,
  routine.getRoutineType match {
    case "SCALAR_FUNCTION" => zetasql.ZetaSQLFunctions.FunctionEnums.Mode.SCALAR
  },
  util.Collections.singletonList(new FunctionSignature(
    new FunctionArgumentType(BigQueryRoutine.determineReturnType(routine)),
    routine.getArguments.asScala
      .map(arg => new zetasql.FunctionArgumentType(convertType(arg.getDataType)))
      .asJava,
    -1L // this parameter seems not to do anything
  ))
)

object BigQueryRoutine {
  val FunctionGroupName = "UDF"

  /**
   * Determines the return type of a given BigQuery routine.
   *
   * If the routine is implemented in JavaScript, this is an easy task, because the BigQuery API will provide the
   * return type.
   *
   * However, if the routine is implemented in SQL, the return type will be determined at query runtime. This means,
   * that we have to analyze the given SQL expression with arguments as catalog constants.
   *
   * @param routine a BigQuery routine
   * @return its return type
   */
  def determineReturnType(routine: bigquery.Routine): zetasql.Type = routine.getLanguage match {
    case "SQL" =>
      val catalog = new zetasql.SimpleCatalog(routine.getRoutineId.getRoutine)
      addBuiltinFunctions(catalog)
      /*
       * The constructor of com.google.zetasql.Constant is package-private, so we create the corresponding proto
       * and use com.google.zetasql.Constant.deserialize to create the actual object.
       */
      routine.getArguments.asScala
        .map(arg => zetasql.SimpleConstantProtos.SimpleConstantProto.newBuilder()
          .addNamePath(arg.getName)
          .setType(convertType(arg.getDataType).serialize())
          .build())
        .map(zetasql.Constant.deserialize(_, ImmutableList.of(), TypeFactory.uniqueNames()))
        .foreach(catalog.addConstant)

      Analyzer.analyzeExpression(routine.getBody, BigQueryAnalyzerOptions, catalog).getType
    case "JAVASCRIPT" => convertType(routine.getReturnType)
  }
}

