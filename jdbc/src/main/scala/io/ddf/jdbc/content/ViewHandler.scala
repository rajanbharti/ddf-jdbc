package io.ddf.jdbc.content

import java.util
import java.util.{Arrays, List => JList}

import com.google.common.base.Joiner
import com.google.common.collect.Lists
import io.ddf.DDF
import io.ddf.content.ViewHandler.{Column, Expression}
import io.ddf.datasource.SQLDataSourceDescriptor
import io.ddf.exception.DDFException
import io.ddf.jdbc.etl.SqlHandler

import scala.collection.JavaConversions._


class ViewHandler(ddf: DDF) extends io.ddf.content.ViewHandler(ddf) {

  val MAX_SAMPLE_SIZE = 1000000

  override def removeColumns(columnNames: util.List[String]): DDF = {
    val columns = ddf.getSchema.getColumns.map { col => col.getName }
    val result: util.List[String] = Lists.newArrayList()
    result.addAll(columns)
    columnNames.foreach { columnName => {
      val it: Iterator[String] = result.iterator
      while (it.hasNext) {
        if (it.next.equalsIgnoreCase(columnName)) {
          it.remove
        }
      }
    }
    }
    val newddf: DDF = this.project(result)
    if (this.getDDF.isMutable) {
      this.getDDF.updateInplace(newddf)
      this.getDDF
    }
    else {
      newddf.getMetaDataHandler.copyFactor(this.getDDF)
      newddf
    }
  }

  override def getRandomSampleByNum(numSamples: Int, withReplaement: Boolean,
                                    seed: Int): DDF = {
    if (numSamples > MAX_SAMPLE_SIZE) {
      throw new IllegalArgumentException("Number of samples is currently " +
        "limited to " + MAX_SAMPLE_SIZE)
    } else {
      val sqlcmd = "SELECT * FROM {1} ORDER BY random() LIMIT " + numSamples
      val ddf = this.getManager.sql2ddf(sqlcmd, new SQLDataSourceDescriptor
      (sqlcmd, null, null, null, this.getDDF().getUUID().toString()))
      ddf
    }
  }

  override def getRandomSample(numSamples: Int, withReplacement: Boolean, seed: Int): java.util.List[Array[Object]] = {
    if (numSamples > MAX_SAMPLE_SIZE) {
      throw new IllegalArgumentException("Number of samples is currently " +
        "limited to " + MAX_SAMPLE_SIZE)
    } else {
      val sqlCmd: String = String.format("SELECT * FROM {1} ORDER BY random()" +
        " LIMIT " + numSamples)

      try {
        val resultDDF: DDF = this.getManager.sql2ddf(sqlCmd, new SQLDataSourceDescriptor(sqlCmd, null, null, null, this.getDDF.getUUID.toString))
        // resultDDF
        val lstRows = resultDDF.getViewHandler.head(numSamples)
        //parse lstString to List[Array[Object]]
        val data = new java.util.ArrayList[Array[Object]]()
        var currentRow = Array[Object]()
        for (row <- lstRows) {
          currentRow = row.split("\t").asInstanceOf[Array[Object]]
          data.add(currentRow)
        }
        data

        //null.asInstanceOf[java.util.List[Array[Object]]]
      }
      catch {
        case e: Exception => {
          throw new DDFException("Unable to query from " + this.getDDF.getTableName, e)
        }
      }
    }
  }

  override def getRandomSample(fraction: Double, withReplacement: Boolean, seed: Int): DDF = {
    if (fraction > 1 || fraction < 0) {
      throw new IllegalArgumentException("Sampling fraction must be from 0 to 1")
    } else {
      null.asInstanceOf[DDF]
    }
  }

  override def _subset(columnExpr: JList[Column], filter: Expression): DDF = {
    updateVectorName(filter, this.getDDF)
    mLog.info("Updated filter: " + filter)
    val colNames: Array[String] = new Array[String](columnExpr.size)
    var i: Int = 0
    while (i < columnExpr.size) {
      updateVectorName(columnExpr.get(i), this.getDDF)
      colNames(i) = columnExpr.get(i).getName
      i += 1;
    }
    mLog.info("Updated columns: " + Arrays.toString(columnExpr.toArray))
    val table = s"(${this.getDDF.getTableName}) ${TableNameGenerator.genTableName(8)}"
    var sqlCmd: String = String.format("SELECT %s FROM %s", colNames.mkString(","), table)
    if (filter != null) {
      sqlCmd = String.format("%s WHERE %s", sqlCmd, filter.toSql)
    }
    mLog.info("sql = {}", sqlCmd)
    val subset: DDF = this.getDDF.getSqlHandler.sql2ddf(sqlCmd)
    return subset
  }
}
