package io.ddf.jdbc;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.jdbc.content.TableNameGenerator;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JdbcDDF extends DDF {

  public JdbcDDF(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace, String name, Schema schema)
      throws DDFException {
    super(manager, data, typeSpecs, namespace, name, schema);
  }

  @Override public DDF copy() throws DDFException {
    return null;
  }

  public JdbcDDF(DDFManager manager, DDFManager defaultManagerIfNull) throws DDFException {
    super(manager, defaultManagerIfNull);
  }


  public JdbcDDF(DDFManager manager) throws DDFException {
    super(manager);
  }




  @Override public List<String> getColumnNames() {
    List<String> colNames = getSchema().getColumnNames();
    List<String> lowerCaseColNames = new ArrayList<>(colNames.size());
    for (String col : colNames) {
      lowerCaseColNames.add(col.toLowerCase());
    }
    return lowerCaseColNames;
  }

  /*
  @Override public String getTableName() {
    if (this.getIsDDFView()) {
      return "(" + super.getTableName() + ") " + TableNameGenerator.genTableName(8);
    } else {
      return super.getTableName();
    }

  }
  */


  /**
   * Class representing column metadata of a JDBC source
   * @TODO: refactor to make it reusable on any JDBC connector
   */
  public class ColumnSchema {

    private String name;
    private Integer colType;

    /*
      Since atm the following variables are not used programmatically,
      I keep it as string to avoid multiple type conversions between layers.
       Note: the output from the JDBC connector is string
     */
    private String isNullable;
    private String isAutoIncrement;
    private String isGenerated;

    public ColumnSchema(String name, Integer colType, String isNullable, String isAutoIncrement, String isGenerated) {
      this.name = name;
      this.colType = colType;
      this.isNullable = isNullable;
      this.isAutoIncrement = isAutoIncrement;
      this.isGenerated = isGenerated;
    }

    public ColumnSchema(String name, Integer colType, String isNullable, String isAutoIncrement) {
      this(name, colType, isNullable, isAutoIncrement, null);
    }

    public String getName() {
      return name;
    }

    /**
     * @Getter and Setter.
     * @return
     */

    Schema.ColumnType getDDFType() throws DDFException {
      //TODO: review data type support
      switch (colType) {
        case Types.ARRAY:
          return Schema.ColumnType.ARRAY;
        case Types.BIGINT:
          return Schema.ColumnType.BIGINT;
        case Types.BINARY:
          return Schema.ColumnType.BINARY;
        case Types.BOOLEAN:
          return Schema.ColumnType.BOOLEAN;
        case Types.CHAR:
          return Schema.ColumnType.STRING;
        case Types.DATE:
          return Schema.ColumnType.DATE;
        case Types.DECIMAL:
          return Schema.ColumnType.DECIMAL;
        case Types.DOUBLE:
          return Schema.ColumnType.DOUBLE;
        case Types.FLOAT:
          return Schema.ColumnType.FLOAT;
        case Types.INTEGER:
          return Schema.ColumnType.INT;
        case Types.LONGVARCHAR:
          return Schema.ColumnType.STRING; //TODO: verify
        case Types.NUMERIC:
          return Schema.ColumnType.DECIMAL;
        case Types.NVARCHAR:
          return Schema.ColumnType.STRING; //TODO: verify
        case Types.SMALLINT:
          return Schema.ColumnType.INT;
        case Types.TIMESTAMP:
          return Schema.ColumnType.TIMESTAMP;
        case Types.TINYINT:
          return Schema.ColumnType.INT;
        case Types.VARCHAR:
          return Schema.ColumnType.STRING; //TODO: verify
        default:
          throw new DDFException(String.format("Type not support %s", JDBCUtils.getSqlTypeName(colType)));
          //TODO: complete for other types
      }
    }

    public Integer getColType() {
      return colType;
    }

    public void setColType(Integer colType) {
      this.colType = colType;
    }

    @Override public String toString() {
      return String.format("[name: %s, type: %s, isNullable: %s, isAutoIncrement: %s, isGenerated: %s]", name, colType,
          isNullable, isAutoIncrement, isGenerated);
    }
  }

  public class TableSchema extends ArrayList<ColumnSchema> {}

  private Schema buildDDFSchema(TableSchema tableSchema) throws DDFException {
    List<Schema.Column> cols = new ArrayList<>();
    Iterator<ColumnSchema> schemaIter = tableSchema.iterator();

    while(schemaIter.hasNext()){
      ColumnSchema jdbcColSchema = schemaIter.next();
      Schema.ColumnType colType = JDBCUtils.getDDFType(jdbcColSchema.getColType()); //TODO: verify if throwing exception makes sense
      String colName = jdbcColSchema.getName();
      cols.add(new Schema.Column(colName, colType));
    }
    return new Schema(null, cols);
  }


}
