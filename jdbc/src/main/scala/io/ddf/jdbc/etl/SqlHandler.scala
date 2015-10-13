package io.ddf.jdbc.etl

import java.io.StringReader
import java.sql.Connection
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

  def getConnection() : Connection = {
    ddfManager.getConnection()
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
        this.getManager.log(">>> Creating view in database")
        val viewName = TableNameGenerator.genTableName(8)
        //View will allow select commands
        DdlCommand(getConnection(), baseSchema, "CREATE VIEW " + viewName + " AS (" +
          command + ")")
        val viewSchema = if (schema == null) catalog.getViewSchema(getConnection(),
          baseSchema, viewName) else schema
        val viewRep = TableNameRepresentation(viewName, viewSchema)
        // TODO(TJ): This function implementation is wrong.
        ddf.getManager.newDDF(this.getManager, viewRep, Array(Representations.VIEW),  ddf.getNamespace, viewName, viewSchema)
      } else {
        this.getManager.log(">>> Creating view in pe/ddf")
        val sqlRet = this.sql("select * from (" + command + ")");
        val schema = sqlRet.getSchema
        schema.setTableName("("+command+")")
        val newDDF = ddf.getManager.newDDF(this.getManager, // the ddfmanager
                                          "this is a view", // the content
                                          // content class
                                          Array(classOf[java.lang.String]),
                                          ddf.getNamespace,
                                          null,
                                          schema)
        if (newDDF == null) {
          this.getManager.log(">>> ERROR: NewDDF is null in sql2ddf")
        } else {
          this.getManager.log(">>> NewDDF sucessfully in sql2ddf")
          if (newDDF.getUUID == null) {
            this.getManager.log(">>> ERROR: uuid is null of ddf")
          } else {
            this.getManager.log(">>> NewDDF UUID ok in sql2ddf")
          }
        }
        // Indicate that this ddf is a view, this information will be handled
        // in TableNameReplacer
        newDDF.setIsDDFView(true)
        newDDF
      }
    }
  }

  def load(command: String): DDF = {
    val l = LoadCommand.parse(command)
    val ddf = ddfManager.getDDFByName(l.tableName)
    val schema = ddf.getSchema
    val tableName = LoadCommand(getConnection(), baseSchema, schema, l)
    val newDDF = ddfManager.getDDFByName(tableName)
    newDDF
  }

  def create2ddf(command: String, schema: Schema): DDF = {
    val tableName = Parsers.parseCreate(command).tableName
    DdlCommand(getConnection(), baseSchema, command)
    val tableSchema = if (schema == null) catalog.getTableSchema(getConnection(),
      baseSchema, tableName) else schema
    val emptyRep = TableNameRepresentation(tableName, tableSchema)
    ddf.getManager.newDDF(this.getManager, emptyRep, Array(Representations.VIEW), ddf.getNamespace, tableName, tableSchema)
  }

  override def sql(command: String): SqlResult = {
    sql(command, Integer.MAX_VALUE, null)
  }

  override def sql(command: String, maxRows: Integer): SqlResult = {
    sql(command, maxRows, null)
  }

  override def sql(command: String, maxRows: Integer, dataSource: DataSourceDescriptor): SqlResult = {
    this.ddfManager.log("run sql in ddf-jdbc, command is : " + command)
    val maxRowsInt: Int = if (maxRows == null) Integer.MAX_VALUE else maxRows
    if (StringUtils.startsWithIgnoreCase(command.trim, "DROP")) {
      DdlCommand(getConnection(), baseSchema, command)
      new SqlResult(null, Collections.singletonList("0"))
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "LOAD")) {
      ddfManager.checkSinkAllowed()
      val l = LoadCommand.parse(command)
      val ddf = ddfManager.getDDFByName(l.tableName)
      val schema = ddf.getSchema
      val tableName = LoadCommand(getConnection(), baseSchema, schema, l)
      new SqlResult(null, Collections.singletonList(tableName))
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "CREATE")) {
      create2ddf(command, null)
      new SqlResult(null, Collections.singletonList("0"))
    } else {
      val tableName = ddf.getSchemaHandler.newTableName()
      SqlCommand(getConnection(), baseSchema, tableName, command, maxRowsInt,
        "\t", this.ddfManager.getEngine)
    }
  }


  override def sqlTyped(command: String): SqlTypedResult = new SqlTypedResult(sql(command))

  override def sqlTyped(command: String, maxRows: Integer): SqlTypedResult = new SqlTypedResult(sql(command, maxRows))

  override def sqlTyped(command: String, maxRows: Integer, dataSource: DataSourceDescriptor): SqlTypedResult = new SqlTypedResult(sql(command, maxRows, dataSource))
}
