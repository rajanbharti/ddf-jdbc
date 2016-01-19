package io.ddf.jdbc.content

import io.ddf.DDF
import io.ddf.content.{RepresentationHandler => RH, _}
import io.ddf.jdbc.JdbcDDFManager

class RepresentationHandler(ddf: DDF) extends RH(ddf) {

  import Representations._

  override def getDefaultDataType: Array[Class[_]] = Array(Representations.VIEW)

  this.addConvertFunction(VIEW_REP, SQL_ARRAY_RESULT_REP, new View2SqlArrayResult(this.ddf))
}


case class TableNameRepresentation(viewName: String, schema: Schema)

case class SqlArrayResult(schema: Schema, result: List[Array[_]])

object Representations {

  val VIEW = classOf[TableNameRepresentation]
  val SQL_ARRAY_RESULT = classOf[SqlArrayResult]

  val SQL_ARRAY_RESULT_REP = new Representation(SQL_ARRAY_RESULT)
  val VIEW_REP = new Representation(VIEW)

  class View2SqlArrayResult(@transient ddf: DDF) extends ConvertFunction(ddf) {
    val ddfManager: JdbcDDFManager = ddf.getManager.asInstanceOf[JdbcDDFManager]
    implicit val catalog = ddfManager.catalog

    override def apply(representation: Representation): Representation = {
      val view = representation.getValue.asInstanceOf[TableNameRepresentation]
      //Override where required
      val sqlArrayResult = SqlArrayResultCommand(ddfManager.getConnection(), ddfManager.baseSchema, ddf.getName, String.format("SELECT * FROM %s", view.viewName))
      new Representation(new SqlArrayResult(view.schema, sqlArrayResult.result), SQL_ARRAY_RESULT)
    }
  }
}

