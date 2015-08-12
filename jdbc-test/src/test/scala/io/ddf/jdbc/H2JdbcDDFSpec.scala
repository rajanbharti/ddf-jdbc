package io.ddf.jdbc

import io.ddf.DDFManager
import io.ddf.jdbc.analytics.AnalyticsBehaviors
import io.ddf.jdbc.content.ContentBehaviors
import io.ddf.jdbc.etl.ETLBehaviors
import org.scalatest.FlatSpec

class H2JdbcDDFSpec extends FlatSpec with AnalyticsBehaviors with ContentBehaviors with ETLBehaviors {
  implicit val loader = H2Loader
  it should behave like ddfWithAddressing
  it should behave like ddfWithAggregationHandler
  it should behave like ddfWithStatisticsHandler
  it should behave like ddfWithBinningHandler

  it should behave like ddfWithMetaDataHandler
  it should behave like ddfWithPersistenceHandler
  it should behave like ddfWithSchemaHandler
  it should behave like ddfWithViewHandler

  it should behave like ddfWithBasicJoinSupport
  it should behave like ddfWithMissingDataDropSupport
  it should behave like ddfWithSqlHandler
  it should behave like ddfWithBasicTransformSupport

}

object ManagerFactory {
  val jdbcDDFManager = DDFManager.get("jdbc").asInstanceOf[JdbcDDFManager]
}

object H2Loader extends Loader {
  override def engine: String = "jdbc"

  override def jdbcDDFManager: JdbcDDFManager = ManagerFactory.jdbcDDFManager
}

