package io.ddf.jdbc


import java.net.URI
import java.sql.{Connection, DriverManager}
import java.util.UUID
import java.util.Properties
import java.util

import com.zaxxer.hikari.{HikariDataSource, HikariConfig}
import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.datasource.{DataSourceDescriptor, JDBCDataSourceCredentials}
import io.ddf.exception.DDFException
import io.ddf.jdbc.content._
import io.ddf.jdbc.etl.SqlHandler
import io.ddf.jdbc.utils.Utils
import io.ddf.misc.Config
import io.ddf.{DDF, DDFManager}
import io.ddf.DDFManager.EngineType
import scalikejdbc.ConnectionPool

import scala.util.{Success, Failure, Try}

class JdbcDDFManager(dataSourceDescriptor: DataSourceDescriptor,
                     engineType: EngineType) extends DDFManager {

  override def getEngine: String = engineType.name()
  this.setEngineType(engineType)
  this.setDataSourceDescriptor(dataSourceDescriptor)
  def catalog: Catalog = SimpleCatalog

  val driverClassName = Config.getValue(getEngine, "jdbcDriverClass")
  Class.forName(driverClassName)
  addRTK()
  var connectionPool = initializeConnectionPool(getEngine)

  val baseSchema = Config.getValue(getEngine, "workspaceSchema")
  val canCreateView = "yes".equalsIgnoreCase(Config.getValue(getEngine, "canCreateView"))

  setEngineType(engineType)
  setDataSourceDescriptor(dataSourceDescriptor)


  def addRTK(): Unit = {
    var jdbcUrl = dataSourceDescriptor.getDataSourceUri.getUri.toString

    if (this.getEngineType.name().equalsIgnoreCase("sfdc")) {
      val rtkString = System.getenv("SFDC_RTK")
      if (rtkString != null) {
        jdbcUrl += "RTK='" + rtkString + "';";
      }
      this.getDataSourceDescriptor.getDataSourceUri().setUri(new URI(jdbcUrl));
    }
  }
  
  def isSinkAllowed = baseSchema != null


  def initializeConnectionPool(engine: String): HikariDataSource = {
    val jdbcUrl = dataSourceDescriptor.getDataSourceUri.getUri.toString
    val credentials = dataSourceDescriptor.getDataSourceCredentials.asInstanceOf[JDBCDataSourceCredentials]
    val jdbcUser = credentials.getUsername
    val jdbcPassword = credentials.getPassword

    val config:HikariConfig = new HikariConfig()
    config.setJdbcUrl(jdbcUrl)
    config.setUsername(jdbcUser)
    config.setPassword(jdbcPassword)

    // We want to retire the connection as soon as possible
    config.setIdleTimeout(if (Config.getValue(getEngine, "jdbcPoolConnIdleTimeoutMs") == null) 10000 else Config.getValue(getEngine, "jdbcPoolConnIdleTimeoutMs").toLong)
    config.setMaxLifetime(if (Config.getValue(getEngine, "jdbcPoolConnMaxLifetimeMs") == null) 20000 else Config.getValue(getEngine, "jdbcPoolConnMaxLifetimeMs").toLong)

    config.setMinimumIdle(if (Config.getValue(getEngine, "jdbcPoolMinIdleConns") == null) 2 else Config.getValue(getEngine, "jdbcPoolMinIdleConns").toInt)
    config.setMaximumPoolSize(if (Config.getValue(getEngine, "maxJDBCPoolSize") == null) 15 else Config.getValue(getEngine, "maxJDBCPoolSize").toInt)
    config.setPoolName(getUUID.toString)
    config.setRegisterMbeans(true)
    val connectionTestQuery = Config.getValue(getEngine, "jdbcConnectionTestQuery")
    if(connectionTestQuery!=null) config.setConnectionTestQuery(connectionTestQuery)
    // This is for pushing prepared statements to Postgres server as in
    // https://jdbc.postgresql.org/documentation/head/server-prepare.html
    //config.addDataSourceProperty("prepareThreshold", 0)
    val pool: HikariDataSource = new HikariDataSource(config)

    // check for valid jdbc login information
    // in case of sfdc
   /*
    val conn = pool.getConnection
    val try_connect = Try(conn.createStatement().execute("SELECT 1"))
    try_connect match {
      case Failure(ex) =>
        if (ex.getMessage.contains("INVALID_LOGIN")) {
          pool.shutdown()
          throw ex
        }

      case Success(_) => // Can execute query, good!!!
    }

    conn.close()
    */
    pool
  }

  def getConnection(): Connection = {
    connectionPool.getConnection
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

  override def transfer(fromEngine: UUID, ddfuuid: UUID): DDF = {
    throw new DDFException("Load DDF from file is not supported!")
  }

  override def transferByTable(fromEngine: UUID, tblName : String): DDF = {
    throw new DDFException("Load DDF from file is not supported!")
  }

  override def getOrRestoreDDF(uuid: UUID): DDF = getDDF(uuid)


  def showTables(schemaName: String): java.util.List[String] = {
    catalog.showTables(getConnection(), schemaName)
  }

  def showViews(schemaName: String): java.util.List[String] = {
    catalog.showViews(getConnection(), schemaName)
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
    connectionPool.shutdown()
  }

  def newDDFFromTable(tblname: String, tgtname: String): DDF = {
    val schema = this.getTableSchema(tblname)
    if (schema == null) {
      throw new DDFException("schema is null in jdbc")
    }
    this.newDDF(this, null, null, "adatao", tgtname, this.getTableSchema(tblname))
  }

}
