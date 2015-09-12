package io.ddf.jdbc


import java.sql.{Connection, DriverManager}
import java.util
import java.util.UUID

import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.datasource.{DataSourceDescriptor, JDBCDataSourceCredentials}
import io.ddf.exception.DDFException
import io.ddf.jdbc.content._
import io.ddf.jdbc.etl.SqlHandler
import io.ddf.jdbc.utils.Utils
import io.ddf.misc.Config
import io.ddf.{DDF, DDFManager}


class JdbcDDFManager(dataSourceDescriptor: DataSourceDescriptor,
                     engineType: String) extends DDFManager {

  override def getEngine: String = engineType

  def catalog: Catalog = SimpleCatalog

  val driverClassName = Config.getValue(getEngine, "jdbcDriverClass")
  Class.forName(driverClassName)
  var connection = initializeConnection(getEngine)
  val baseSchema = Config.getValue(getEngine, "workspaceSchema")
  val canCreateView = Config.getValue(getEngine, "canCreateView")
    .equalsIgnoreCase("yes")
  setEngineType(engineType)
  setDataSourceDescriptor(dataSourceDescriptor)

  def isSinkAllowed = baseSchema != null

  def initializeConnection(engine: String) = {
    val jdbcUrl = dataSourceDescriptor.getDataSourceUri.getUri.toString
    val credentials = dataSourceDescriptor.getDataSourceCredentials.asInstanceOf[JDBCDataSourceCredentials]
    val jdbcUser = credentials.getUsername
    val jdbcPassword = credentials.getPassword
    DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
  }

  def getConnection(): Connection = {
    if (connection != null && !connection.isValid(10)) {
      connection = this.initializeConnection(getEngine)
    }
    connection
  }

  def getCanCreateView(): Boolean = {
    canCreateView
  }

  def drop(command: String) = {
    checkSinkAllowed()
    implicit val cat = catalog
    DdlCommand(getConnection(), baseSchema, command)
  }

  def create(command: String) = {
    checkSinkAllowed()
    val sqlHandler = this.getDummyDDF.getSqlHandler.asInstanceOf[SqlHandler]
    checkSinkAllowed()
    implicit val cat = catalog
    sqlHandler.create2ddf(command, null)
  }

  def load(command: String) = {
    checkSinkAllowed()
    val l = LoadCommand.parse(command)
    val ddf = getDDFByName(l.tableName)
    val schema = ddf.getSchema
    implicit val cat = catalog
    LoadCommand(getConnection(), baseSchema, schema, l)
    ddf
  }

  override def loadTable(fileURL: String, fieldSeparator: String): DDF = {
    checkSinkAllowed()
    implicit val cat = catalog
    val tableName = getDummyDDF.getSchemaHandler.newTableName()
    val load = new Load(tableName, fieldSeparator.charAt(0), fileURL, null, null, true)
    val lines = LoadCommand.getLines(load, 5)
    import scala.collection.JavaConverters._
    val colInfo = getColumnInfo(lines.asScala.toList, hasHeader = false, doPreferDouble = true)
    val schema = new Schema(tableName, colInfo)
    val createCommand = SchemaToCreate(schema)
    val ddf = create(createCommand)
    LoadCommand(getConnection(), baseSchema, schema, load)
    ddf
  }

  def checkSinkAllowed(): Unit = {
    if (!isSinkAllowed) throw new DDFException("Cannot load table into database as workSpace is not configured")
  }

  def getColumnInfo(sampleData: List[Array[String]],
                    hasHeader: Boolean = false,
                    doPreferDouble: Boolean = true): Array[Schema.Column] = {

    val sampleSize: Int = sampleData.length
    mLog.info("Sample size: " + sampleSize)

    val firstRow: Array[String] = sampleData.head

    val headers: Seq[String] = if (hasHeader) {
      firstRow.toSeq
    } else {
      val size: Int = firstRow.length
      (1 to size) map (i => s"V$i")
    }

    val sampleStrings = if (hasHeader) sampleData.tail else sampleData

    val samples = sampleStrings.toArray.transpose

    samples.zipWithIndex.map {
      case (col, i) => new Schema.Column(headers(i), Utils.determineType(col, doPreferDouble, false))
    }
  }

  override def getOrRestoreDDFUri(ddfURI: String): DDF = null

  override def transfer(fromEngine: String, ddfuri: String): DDF = {
    throw new DDFException("Load DDF from file is not supported!")
  }

  override def getOrRestoreDDF(uuid: UUID): DDF = null


  def showTables(schemaName: String): java.util.List[String] = {
    catalog.showTables(getConnection(), schemaName)
  }

  def getTableSchema(tableName: String) = {
    catalog.getTableSchema(getConnection(), null, tableName)
  }

  def showDatabases(): java.util.List[String] = {
    catalog.showDatabases(getConnection())
  }

  def setDatabase(database: String) : Unit = {
    catalog.setDatabase(getConnection(), database)
  }

  def listColumnsForTable(schemaName: String,
                          tableName: String): util.List[Column] = {
    this.catalog.listColumnsForTable(getConnection(), schemaName, tableName);
  }

  def showSchemas(): util.List[String] = {
    this.catalog.showSchemas(getConnection())
  }

  def setSchema(schemaName: String): Unit = {
    this.catalog.setSchema(getConnection(), schemaName)
  }

  def disconnect() = {
    connection.close()
  }
}
