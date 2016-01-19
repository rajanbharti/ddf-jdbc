package io.ddf.aws.ml

import java.lang

import com.amazonaws.services.machinelearning.model.MLModelType
import io.ddf.DDF
import io.ddf.aws.AWSDDFManager
import io.ddf.aws.ml.util.CrossValidation
import io.ddf.exception.DDFException
import io.ddf.jdbc.content.{DdlCommand, Representations, SqlArrayResult}
import io.ddf.misc.ADDFFunctionalGroupHandler
import io.ddf.ml.{CrossValidationSet, IModel, ISupportML}

import scala.collection.JavaConverters._

class MLSupporter(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with ISupportML with Serializable {

  val ddfManager = ddf.getManager.asInstanceOf[AWSDDFManager]
  val awsProperties = AwsConfig.getPropertiesForAmazonML(ddfManager)
  val awsHelper = new AwsHelper(awsProperties.s3Properties)
  val awsMLHelper = new AwsMLHelper(awsProperties)

  def getAwsMLHelper = awsMLHelper

  def getAwsHelper = awsHelper

  override def applyModel(model: IModel): DDF = applyModel(model, hasLabels = true)

  override def applyModel(model: IModel, hasLabels: Boolean): DDF = applyModel(model, hasLabels, includeFeatures = true)

  override def applyModel(model: IModel, hasLabels: Boolean, includeFeatures: Boolean): DDF = {
    val awsModel = model.getRawModel.asInstanceOf[AwsModel]
    val tableName = ddf.getTableName
    val sql = awsHelper.selectSql(tableName)
    val dataSourceId = awsMLHelper.createDataSourceFromRedShift(ddf.getSchema, sql, awsModel.getMLModelType)
    //this will wait for batch prediction to complete
    val batchId = awsMLHelper.createBatchPrediction(awsModel, dataSourceId)
    //last column is target column for supervised learning
    val targetColumn = ddf.getSchema.getColumns.asScala.last
    val uniqueTargetVal = if (awsModel.getMLModelType equals MLModelType.MULTICLASS) {
      ddf.setAsFactor(targetColumn.getName)
      ddf.getSchemaHandler.computeFactorLevelsAndLevelCounts()
      ddf.getSchema.getColumn(targetColumn.getName).getOptionalFactor.getLevels.asScala.map(value => "col_" + value + " " +
        "float8").mkString(",")
    }
    else ""
    val newTableName = Identifiers.newTableName
    val createTableSql = awsMLHelper.createTableSqlForModelType(awsModel.getMLModelType, newTableName, uniqueTargetVal)
    val newDDF = ddf.getManager.asInstanceOf[AWSDDFManager].create(createTableSql)
    //now copy the results to redshift
    val manifestPath = awsHelper.createResultsManifestForRedshift(batchId)
    val sqlToCopy = awsHelper.sqlToCopyFromS3ToRedshift(manifestPath, newTableName)
    implicit val cat = ddfManager.catalog
    DdlCommand(ddfManager.getConnection(), ddfManager.baseSchema, sqlToCopy)
    //and return the ddf
    newDDF
  }

  override def CVRandom(k: Int, trainingSize: Double, seed: lang.Long): java.util.List[CrossValidationSet] = {
    val crossValidation: CrossValidation = new CrossValidation(ddf)
    crossValidation.CVRandom(k, trainingSize)
  }

  override def CVKFold(k: Int, seed: lang.Long): java.util.List[CrossValidationSet] = {
    val crossValidation: CrossValidation = new CrossValidation(ddf)
    crossValidation.CVK(k)
  }

  override def train(trainMethodKey: String, args: AnyRef*): IModel = {
    val sql = awsHelper.selectSql(ddf.getTableName)
    val mlModelType = try {
      MLModelType.valueOf(trainMethodKey)
    }
    catch {
      case e: IllegalArgumentException => throw new DDFException(e)
    }

    val dataSourceId = awsMLHelper.createDataSourceFromRedShift(ddf.getSchema, sql, mlModelType)
    val paramsMap = if (args.length < 1 || args(0) == null) {
      new java.util.HashMap[String, String]()
    } else {
      args(0).asInstanceOf[java.util.Map[String, String]]
    }

    //this will wait for model creation to complete
    val modelId = awsMLHelper.createModel(dataSourceId, mlModelType, paramsMap)

    mlModelType match {
      case MLModelType.BINARY => Model(BinaryClassification(awsMLHelper, ddf.getSchema, modelId, mlModelType))
      case MLModelType.MULTICLASS => Model(MultiClassClassification(awsMLHelper, ddf.getSchema, modelId, mlModelType))
      case MLModelType.REGRESSION => Model(LinearRegression(awsMLHelper, ddf.getSchema, modelId, mlModelType))
    }
  }

  def getConfusionMatrix(iModel: IModel, v: Double): Array[Array[Long]] = {
    if (iModel.getRawModel.asInstanceOf[AwsModel].getMLModelType != MLModelType.REGRESSION) {
      throw new DDFException("Confusion Matrix can only be evaluated for Regression model")
    }
    val predictedDDF = ddf.ML.applyModel(iModel)
    val matrix = Array.ofDim[Long](2, 2)

    val predictDDFAsSql = predictedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
      .asInstanceOf[SqlArrayResult].result
    val result = predictDDFAsSql map (row => row(row.length - 2).asInstanceOf[Double]) zip (predictDDFAsSql map (row
    => row(row.length - 1).asInstanceOf[Double]))

    for (row <- result.indices) {
      val (oldVal,newVal) = result(row)
      if ((oldVal < v || oldVal == v) && (newVal < v || newVal == v))
        matrix(0)(0) = matrix(0)(0) + 1
      else if ((oldVal < v || oldVal == v) && newVal > v)
        matrix(0)(1) = matrix(0)(1) + 1
      else if (oldVal > v && (newVal < v || newVal == v))
        matrix(1)(0) = matrix(1)(0) + 1
      else
        matrix(1)(1) = matrix(1)(1) + 1
    }
    matrix
  }


}
