package io.ddf.postgres

import java.sql.{Connection, DatabaseMetaData, ResultSet}
import java.util

import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.content.Schema.ColumnType
import io.ddf.datasource.DataSourceDescriptor
import io.ddf.DDFManager.EngineType
import io.ddf.jdbc.JdbcDDFManager
import io.ddf.jdbc.content.{Catalog, SqlArrayResultCommand}
import io.ddf.jdbc.utils.Utils
import scalikejdbc.{DB, SQL}

class PostgresDDFManager(dataSourceDescriptor: DataSourceDescriptor,
                         engineType: EngineType,
                         uri: String)
  extends JdbcDDFManager(dataSourceDescriptor, engineType, uri) {

  def this(dataSourceDescriptor: DataSourceDescriptor, engineType: EngineType) {
    this(dataSourceDescriptor, engineType, null)
  }

  def this(engineType: EngineType, uri: String) {
    this(null, engineType, uri)
  }

  override def getEngine = engineType.name()

  override def catalog = PostgresCatalog

  catalog.curSchema = this.baseSchema

  override def setSchema(schemaName: String): Unit = {
    this.sql("set search_path to " + schemaName)
  }

  override def showTables(schemaName: String): java.util.List[String] = {
    val tables = catalog.showTables(getConnection, schemaName)
    val views = catalog.showViews(getConnection, schemaName)
    tables.removeAll(views)
    tables
  }

}


object PostgresCatalog extends Catalog {

  var curSchema : String = "public"

  def log(str: String): Unit = {
    this.mLog.info(str)
  }

  override def getViewSchema(connection: Connection, schemaName: String, name: String): Schema = getTableSchema(connection, schemaName, name)

  override def getTableSchema(connection: Connection, schemaName: String, name: String): Schema = {
    implicit val catalog = this
    val sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '" + schemaName.toLowerCase + "' AND table_name ='" + name.toLowerCase + "' order by ordinal_position"
    val columns: util.List[Column] = new util.ArrayList[Column]
    val sqlResult = SqlArrayResultCommand(connection, "information_schema", name, sql)
    sqlResult.result.foreach { row =>
      val columnName = row(0).toString
      var columnTypeStr = row(1).toString

      this.mLog.info("list columns: " + columnName + " " + columnTypeStr)
      val column = new Column(columnName, this.getColumnType(columnTypeStr))
      columns.add(column)
    }
    new Schema(name, columns)
  }

  override def showViews(connection: Connection, schemaName: String): util.List[String] = {
    var schema: String = schemaName
    if (schema == null) {
      schema = curSchema
    }
    val tables: util.List[String] = new util.ArrayList[String]
    var rs: ResultSet = null
    try {
      val metadata: DatabaseMetaData = connection.getMetaData
      rs = metadata.getTables(null, schema, null, Array("VIEW"))
      while (rs.next()) {
        tables.add(rs.getString("TABLE_NAME"))
      }
    } finally {
      if (rs != null) rs.close()
      connection.close()
    }

    tables
  }

  override def showTables(connection: Connection, schemaName: String): util.List[String] = {
      var schema: String = schemaName
      if (schema == null) {
        schema = curSchema
      }
      val sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema.toLowerCase + "'"
    val tables: util.List[String] = new util.ArrayList[String]
    implicit val catalog = this
    val sqlResult = SqlArrayResultCommand(connection, "information_schema", "tables", sql)
    sqlResult.result.foreach(row => tables.add(row(0).toString))
    tables
  }

  override def showDatabases(connection: Connection): util.List[String] = {
    val sql = "SELECT datname FROM pg_database WHERE datistemplate = false;";
    val databases: util.List[String] = new util.ArrayList[String]
    implicit  val catalog = this
    val sqlResult = SqlArrayResultCommand(connection, "pg_database",
      "tables", sql)
    sqlResult.result.foreach(row => databases.add(row(0).toString))
    databases
  }

  override def setDatabase(connection: Connection, database: String): Unit = {
    connection.setCatalog(database)
  }

  override  def listColumnsForTable(connection: Connection, schemaName: String,
                                    tableName: String): util.List[Column] = {
    val columns: util.List[Column] = new util.ArrayList[Column]
    var resultSet:ResultSet = null
    try {
      val metadata: DatabaseMetaData = connection.getMetaData
      resultSet = metadata.getColumns(null, schemaName, tableName, null)
      while (resultSet.next) {
        val columnName = resultSet.getString(4)
        var columnType = resultSet.getInt(5)
        this.mLog.info("list columns: " + columnName + " " + columnType);
        val column = new Column(columnName, Utils.getDDFType(columnType))
        columns.add(column)
      }
    } finally {
      if (resultSet!= null) resultSet.close()
      connection.close()
    }
    columns
  }

  override def showSchemas(connection: Connection): util.List[String] = {
    var rs:ResultSet = null
    val schemas: util.List[String] = new util.ArrayList[String]()
    try {
      rs = connection.getMetaData.getSchemas()
      while (rs.next()) {
        schemas.add(rs.getString("TABLE_SCHEM"))
      }
    } finally {
      if (rs != null) rs.close()
      connection.close()
    }
    schemas
  }

  override def setSchema(connection: Connection, schemaName: String): Unit = {
      implicit val session = DB(connection).autoCommitSession()
      SQL("set search_path to " + schemaName).execute()
  }


  override def getColumnType(typeStr: String) : ColumnType = {
    typeStr match {
      case "int4" => ColumnType.INT
      case "integer" => ColumnType.INT
      case "int8"=> ColumnType.BIGINT
      case "float4"=> ColumnType.FLOAT
      case "float8" => ColumnType.DOUBLE
      case "varchar"=> ColumnType.STRING
      case "varchar2"=> ColumnType.STRING
      case "double precision"=> ColumnType.DOUBLE
      case "name"=> ColumnType.STRING
      case "text"=> ColumnType.STRING
      case "date"=> ColumnType.DATE
      case "bool"=> ColumnType.BOOLEAN
      case "character varying" =>ColumnType.STRING
      case "numeric" =>ColumnType.DOUBLE
      case "bigint" => ColumnType.BIGINT
    }
  }
}
