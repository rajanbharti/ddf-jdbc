package io.ddf.jdbc.etl

import java.io.StringReader
import java.util.Collections

import io.ddf.content.{Schema, SqlResult, SqlTypedResult}
import io.ddf.datasource.{DataFormat, DataSourceDescriptor, JDBCDataSourceDescriptor, SQLDataSourceDescriptor}
import io.ddf.exception.DDFException
import io.ddf.jdbc.JdbcDDFManager
import io.ddf.jdbc.content._
import io.ddf.{DDF, TableNameReplacer}
import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select
import org.apache.commons.lang.StringUtils


class SqlHandler(ddf: DDF) extends io.ddf.etl.ASqlHandler(ddf) {

  val ddfManager: JdbcDDFManager = ddf.getManager.asInstanceOf[JdbcDDFManager]
  val baseSchema = ddfManager.baseSchema

  implicit val catalog = ddfManager.catalog
  val connection = ddfManager.connection

  @throws[DDFException]
  override def sql2ddfHandle(command: String, schema: Schema, dataSource: DataSourceDescriptor, dataFormat: DataFormat, tableNameReplacer: TableNameReplacer): DDF = {
    ddfManager.checkSinkAllowed()
    if (dataSource != null) {
      val sqlDataSourceDescriptor: SQLDataSourceDescriptor = dataSource.asInstanceOf[SQLDataSourceDescriptor]
      if (sqlDataSourceDescriptor == null) {
        throw new DDFException("ERROR: Handling datasource")
      }
      if (dataSource.isInstanceOf[JDBCDataSourceDescriptor] || sqlDataSourceDescriptor.getDataSource != null) {
        return this.sql2ddf(command, schema, dataSource, dataFormat)
      }
    }
    val parserManager: CCJSqlParserManager = new CCJSqlParserManager
    val reader: StringReader = new StringReader(command)
    try {
      var statement: Statement = parserManager.parse(reader)
      if (!(statement.isInstanceOf[Select])) {
        throw new DDFException("ERROR: Only select is allowed in this sql2ddf")
      }
      else {
        this.mLog.info("replace: " + command)
        statement = tableNameReplacer.run(statement)
        if (tableNameReplacer.containsLocalTable || tableNameReplacer
          .mUri2TblObj.size == 1) {
          this.mLog.info("New stat is " + statement.toString)
          return this.sql2ddf(statement.toString, schema, dataSource, dataFormat)
        }
        else {
          val selectString: String = statement.toString
          val ddf: DDF = this.getManager.transferByTable(tableNameReplacer.fromEngineName, selectString)
          return ddf
        }
      }
    }
    catch {
      case e: JSQLParserException => {
        throw new DDFException(" SQL Syntax ERROR: " + e.getCause.getMessage.split("\n")(0))
      }
      case e: DDFException => {
        throw e
      }
      case e: Exception => {
        throw new DDFException(e)
      }
    }
  }

  override def sql2ddf(command: String): DDF = {
    this.sql2ddf(command, null, null, null)
  }

  override def sql2ddf(command: String, schema: Schema): DDF = {
    this.sql2ddf(command, schema, null, null)
  }

  override def sql2ddf(command: String, dataFormat: DataFormat): DDF = {
    this.sql2ddf(command, null, null, null)
  }

  override def sql2ddf(command: String, schema: Schema, dataSource: DataSourceDescriptor): DDF = {
    this.sql2ddf(command, schema, dataSource, null)
  }

  override def sql2ddf(command: String, schema: Schema, dataFormat: DataFormat): DDF = {
    this.sql2ddf(command, schema, null, null)
  }

  override def sql2ddf(command: String, schema: Schema, dataSource: DataSourceDescriptor, dataFormat: DataFormat): DDF = {
    ddfManager.checkSinkAllowed()
    if (StringUtils.startsWithIgnoreCase(command.trim, "LOAD")) {
      load(command)
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "CREATE")) {
      create2ddf(command, schema)
    } else {
      if (this.ddfManager.getCanCreateView()) {
        val viewName = genTableName(8)
        //View will allow select commands
        DdlCommand(connection, baseSchema, "CREATE VIEW " + viewName + " AS (" + command + ")")
        val viewSchema = if (schema == null) catalog.getViewSchema(connection, baseSchema, viewName) else schema
        val viewRep = TableNameRepresentation(viewName, viewSchema)
        ddf.getManager.newDDF(this.getManager, viewRep, Array(Representations.VIEW), this.getManager.getEngineName, ddf.getNamespace, viewName, viewSchema)
      } else {
        //print out some error
        //TODO log this properly
        print("Error: can't created view")
        return null

      }
    }
  }

  def load(command: String): DDF = {
    val l = LoadCommand.parse(command)
    val ddf = ddfManager.getDDFByName(l.tableName)
    val schema = ddf.getSchema
    val tableName = LoadCommand(connection, baseSchema, schema, l)
    val newDDF = ddfManager.getDDFByName(tableName)
    newDDF
  }

  def create2ddf(command: String, schema: Schema): DDF = {
    val tableName = Parsers.parseCreate(command).tableName
    DdlCommand(connection, baseSchema, command)
    val tableSchema = if (schema == null) catalog.getTableSchema(connection, baseSchema, tableName) else schema
    val emptyRep = TableNameRepresentation(tableName, tableSchema)
    ddf.getManager.newDDF(this.getManager, emptyRep, Array(Representations.VIEW), this.getManager.getEngineName, ddf.getNamespace, tableName, tableSchema)
  }

  override def sql(command: String): SqlResult = {
    sql(command, Integer.MAX_VALUE, null)
  }

  override def sql(command: String, maxRows: Integer): SqlResult = {
    sql(command, Integer.MAX_VALUE, null)
  }

  override def sql(command: String, maxRows: Integer, dataSource: DataSourceDescriptor): SqlResult = {
    this.ddfManager.log("run sql in ddf-jdbc, command is : " + command)
    val maxRowsInt: Int = if (maxRows == null) Integer.MAX_VALUE else maxRows
    if (StringUtils.startsWithIgnoreCase(command.trim, "DROP")) {
      DdlCommand(connection, baseSchema, command)
      new SqlResult(null, Collections.singletonList("0"))
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "LOAD")) {
      ddfManager.checkSinkAllowed()
      val l = LoadCommand.parse(command)
      val ddf = ddfManager.getDDFByName(l.tableName)
      val schema = ddf.getSchema
      val tableName = LoadCommand(connection, baseSchema, schema, l)
      new SqlResult(null, Collections.singletonList(tableName))
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "CREATE")) {
      create2ddf(command, null)
      new SqlResult(null, Collections.singletonList("0"))
    } else {
      val tableName = ddf.getSchemaHandler.newTableName()
      SqlCommand(connection, baseSchema, tableName, command, maxRowsInt,
        "\t", this.ddfManager.getEngine)
    }
  }

  val possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
  val possibleText = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

  def genTableName(length: Int) = {
    def random(possible: String) = possible.charAt(Math.floor(Math.random() * possible.length).toInt)
    val text = new StringBuffer
    var i = 0
    while (i < length) {
      if (i == 0)
        text.append(random(possibleText))
      else
        text.append(random(possible))
      i = i + 1
    }
    text.toString
  }

  override def sqlTyped(command: String): SqlTypedResult = new SqlTypedResult(sql(command))

  override def sqlTyped(command: String, maxRows: Integer): SqlTypedResult = new SqlTypedResult(sql(command, maxRows))

  override def sqlTyped(command: String, maxRows: Integer, dataSource: DataSourceDescriptor): SqlTypedResult = new SqlTypedResult(sql(command, maxRows, dataSource))
}
