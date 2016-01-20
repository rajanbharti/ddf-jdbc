package io.ddf.jdbc.content

import java.lang.{Integer => JInt}
import java.util
import java.util.{HashMap => JHMap, List => JList, Map => JMap}

import io.ddf.content.Schema
import io.ddf.content.Schema.ColumnType
import io.ddf.datasource.SQLDataSourceDescriptor
import io.ddf.exception.DDFException
import io.ddf.{DDF, Factor}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class SchemaHandler(ddf: DDF) extends io.ddf.content.SchemaHandler(ddf: DDF) {

  @throws(classOf[DDFException])
  override def setFactorLevelsForStringColumns(xCols: Array[String]) {
    xCols.foreach { col =>
      val c = this.getColumn(col)
      if (c.getType eq Schema.ColumnType.STRING) {
        this.setAsFactor(c.getName)
      }
    }
  }

  @throws(classOf[DDFException])
  override def computeFactorLevelsAndLevelCounts = {
    val columnIndexes: util.List[Integer] = new util.ArrayList[Integer]
    val columnTypes: util.List[Schema.ColumnType] = new util.ArrayList[Schema.ColumnType]
    import scala.collection.JavaConversions._
    for (col <- this.getColumns) {
      if (col.getColumnClass eq Schema.ColumnClass.FACTOR) {
        var colFactor: Factor[_] = col.getOptionalFactor
        if (colFactor == null || colFactor.getLevelCounts == null || colFactor.getLevels == null) {
          if (colFactor == null) {
            colFactor = this.setAsFactor(col.getName)
          }
          columnIndexes.add(this.getColumnIndex(col.getName))
          columnTypes.add(col.getType)
        }
      }
    }
    //loop through all factors and compute factor


    columnIndexes.foreach(colIndex => {
      val col = this.getColumn(this.getColumnName(colIndex))

      val quotedColName = "\"" + col.getName() + "\""
      val command = s"select ${quotedColName}, count(${quotedColName}) from " +
        s"{1} group by ${quotedColName}"

      val sqlResult = this.getManager.sql(command, new SQLDataSourceDescriptor(command, null, null, null, this.getDDF.getUUID.toString))
      //JMap[String, Integer]
      var result = sqlResult.getRows()
      val levelCounts: java.util.Map[String, Integer] = new java.util.HashMap[String, Integer]()
      for (item <- result) {
        if (item.split("\t").length > 1)
          levelCounts.put(item.split("\t")(0), Integer.parseInt(item.split("\t")(1)))
        else //todo log this properly
          this.mLog.debug("exception parsing item")
        this.mLog.debug(item)
      }

      if (levelCounts != null) {
        val factor: Factor[_] = col.getOptionalFactor
        val levels: util.List[String] = new util.ArrayList[String](levelCounts.keySet)
        factor.setLevelCounts(levelCounts)
        factor.setLevels(levels, false)
      }
    })
  }
}

/**
  */
object GetMultiFactor {

  //For Java interoperability
  def getFactorCounts[T](rdd: JList[Array[_]], columnIndexes: JList[JInt], columnTypes: JList[ColumnType], rddUnit: Class[T]):
  JMap[JInt, JMap[String, JInt]] = {
    getFactorCounts(rdd.asScala.toList, columnIndexes, columnTypes)(ClassTag(rddUnit))
  }

  def getFactorCounts[T](rdd: List[Array[_]], columnIndexes: JList[JInt], columnTypes: JList[ColumnType])(implicit tag: ClassTag[T]): JMap[JInt, JMap[String, JInt]] = {
    val columnIndexesWithTypes = (columnIndexes zip columnTypes).toList

    tag.runtimeClass match {
      case arrObj if arrObj == classOf[Array[Object]] =>
        val mapper = new ArrayObjectMultiFactorMapper(columnIndexesWithTypes)
        rdd.map(mapper).reduce(new MultiFactorReducer)
      case _ =>
        throw new DDFException("Cannot get multi factor for Result[%s]".format(tag.toString))
    }
  }


  class ArrayObjectMultiFactorMapper(indexesWithTypes: List[(JInt, ColumnType)])
    extends (Array[_] => JMap[JInt, JMap[String, JInt]]) with Serializable {
    @Override
    def apply(row: Array[_]): JMap[JInt, JMap[String, JInt]] = {
      val aMap: JMap[JInt, JMap[String, JInt]] = new java.util.HashMap[JInt, JMap[String, JInt]]()
      val typeIter = indexesWithTypes.iterator
      while (typeIter.hasNext) {
        val (idx, typ) = typeIter.next()
        val value: Option[String] = Option(row(idx)) match {
          case Some(x) => typ match {
            case ColumnType.INT => Option(x.asInstanceOf[Int].toString)
            case ColumnType.DOUBLE => Option(x.asInstanceOf[Double].toString)
            case ColumnType.STRING => Option(x.asInstanceOf[String])
            case ColumnType.FLOAT => Option(x.asInstanceOf[Float].toString)
            case ColumnType.BIGINT => Option(x.asInstanceOf[Long].toString)
            case unknown => x match {
              case y: java.lang.Integer => Option(y.toString)
              case y: java.lang.Double => Option(y.toString)
              case y: java.lang.String => Option(y)
              case y: java.lang.Long => Option(y.toString)
            }
          }
          case None => None
        }
        value match {
          case Some(string) => {
            Option(aMap.get(idx)) match {
              case Some(map) => {
                val num = map.get(string)
                map.put(string, if (num == null) 1 else num + 1)
              }
              case None => {
                val newMap = new JHMap[String, JInt]()
                newMap.put(string, 1)
                aMap.put(idx, newMap)
              }
            }
          }
          case None =>
        }
      }
      aMap
    }
  }


  class MultiFactorReducer
    extends ((JMap[JInt, JMap[String, JInt]], JMap[JInt, JMap[String, JInt]]) => JMap[JInt, JMap[String, JInt]])
    with Serializable {
    @Override
    def apply(map1: JMap[JInt, JMap[String, JInt]], map2: JMap[JInt, JMap[String, JInt]]): JMap[JInt, JMap[String, JInt]] = {
      for ((idx, smap1) <- map1) {

        Option(map2.get(idx)) match {
          case Some(smap2) =>
            for ((string, num) <- smap1) {
              val aNum = smap2.get(string)
              smap2.put(string, if (aNum == null) num else (aNum + num))
            }
          case None => map2.put(idx, smap1)
        }
      }
      map2
    }
  }

}
