package io.ddf.jdbc;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.jdbc.content.TableNameGenerator;

import java.util.ArrayList;
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
}
