package io.ddf.jdbc.content

import java.io.File
import java.util

import io.ddf.content.APersistenceHandler.PersistenceUri
import io.ddf.content.Schema.Column
import io.ddf.jdbc.{BaseBehaviors, Loader}
import io.ddf.{DDF, DDFManager}
import org.scalatest.FlatSpec

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


trait ContentBehaviors extends BaseBehaviors {
  this: FlatSpec =>

  def ddfWithAddressing(implicit l: Loader): Unit = {
    val ddf = l.loadAirlineDDF()

    it should "load data from file" in {
      ddf.getNamespace should be("adatao")
      ddf.getColumnNames should have size 29

    }

    it should "load data from file using loadTable" in {
      val filePath = getClass.getResource("/airline.csv").getPath
      val ddf = l.jdbcDDFManager.loadTable(filePath, ",")
      ddf.getNamespace should be("adatao")
      ddf.getColumnNames should have size 29

    }

    it should "be addressable via URI" in {
      ddf.getUri should be("ddf://" + ddf.getNamespace + "/" + ddf.getName)
      l.jdbcDDFManager.getDDFByURI("ddf://" + ddf.getNamespace + "/" + ddf.getName) should be(ddf)
    }
  }

  def ddfWithMetaDataHandler(implicit l: Loader): Unit = {
    it should "get number of rows" in {
      val ddf = l.loadAirlineDDF()
      ddf.getNumRows should be(31)
    }
  }

  def ddfWithPersistenceHandler(implicit l: Loader): Unit = {
    it should "hold namespaces correctly" in {
      val manager: DDFManager = DDFManager.get(l.engine,l.jdbcDDFManager.getDataSourceDescriptor)
      val ddf: DDF = manager.newDDF

      val namespaces = ddf.getPersistenceHandler.listNamespaces

      namespaces should not be null
      for (namespace <- namespaces.asScala) {
        val ddfs = ddf.getPersistenceHandler.listItems(namespace)
        ddfs should not be null
      }

    }

    ignore should "persist and unpersist a jdbc DDF" in {
      val manager: DDFManager = DDFManager.get(l.engine,l.jdbcDDFManager.getDataSourceDescriptor)
      val ddf: DDF = manager.newDDF
      val uri: PersistenceUri = ddf.persist
      uri.getEngine should be(l.engine.toString)
      new File(uri.getPath).exists() should be(true)
      ddf.unpersist()
    }
  }

  def ddfWithSchemaHandler(implicit l: Loader): Unit = {
    val manager = l.jdbcDDFManager
    val ddf = l.loadAirlineDDF()
    it should "get schema" in {
      ddf.getSchema should not be null
    }

    it should "get columns" in {
      val columns: util.List[Column] = ddf.getSchema.getColumns
      columns should not be null
      columns.length should be(29)
    }

    it should "get columns for sql2ddf create table" in {
      val ddf = l.loadAirlineDDF()
      val columns = ddf.getSchema.getColumns
      columns should not be null
      columns.length should be(29)
      columns.head.getName.toUpperCase should be("YEAR")
    }

    it should "test get factors on DDF" in {
      val ddf = l.loadMtCarsDDF()
      val schemaHandler = ddf.getSchemaHandler
      Array(7, 8, 9, 10).foreach {
        idx => schemaHandler.setAsFactor(idx)
      }
      schemaHandler.computeFactorLevelsAndLevelCounts()
      val cols = Array(7, 8, 9, 10).map {
        idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
      }
      println("", cols.mkString(","))
      assert(cols(0).getOptionalFactor.getLevelCounts.get("1") === 14)
      assert(cols(0).getOptionalFactor.getLevelCounts.get("0") === 18)
      assert(cols(1).getOptionalFactor.getLevelCounts.get("1") === 13)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("4") === 12)

      assert(cols(2).getOptionalFactor.getLevelCounts.get("3") === 15)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("5") === 5)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("1") === 7)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("2") === 10)
    }


    it should "test get factors" in {
      val ddf = manager.sql2ddf("select * from ddf://adatao/mtcars")

      val schemaHandler = ddf.getSchemaHandler

      Array(7, 8, 9, 10).foreach {
        idx => schemaHandler.setAsFactor(idx)
      }
      schemaHandler.computeFactorLevelsAndLevelCounts()

      val cols2 = Array(7, 8, 9, 10).map {
        idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
      }

      assert(cols2(0).getOptionalFactor.getLevelCounts.get("1") === 14)
      assert(cols2(0).getOptionalFactor.getLevelCounts.get("0") === 18)
      assert(cols2(1).getOptionalFactor.getLevelCounts.get("1") === 13)
      assert(cols2(2).getOptionalFactor.getLevelCounts.get("4") === 12)

      assert(cols2(2).getOptionalFactor.getLevelCounts.get("3") === 15)
      assert(cols2(2).getOptionalFactor.getLevelCounts.get("5") === 5)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("1") === 7)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("2") === 10)
    }

    it should "test NA handling" in {
      val ddf = l.loadAirlineNADDF()
      val schemaHandler = ddf.getSchemaHandler

      Array(0, 8, 16, 17, 24, 25).foreach {
        idx => schemaHandler.setAsFactor(idx)
      }
      schemaHandler.computeFactorLevelsAndLevelCounts()

      val cols = Array(0, 8, 16, 17, 24, 25).map {
        idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
      }
      assert(cols(0).getOptionalFactor.getLevels.contains("2008"))
      assert(cols(0).getOptionalFactor.getLevels.contains("2010"))
      assert(cols(0).getOptionalFactor.getLevelCounts.get("2008") === 28.0)
      assert(cols(0).getOptionalFactor.getLevelCounts.get("2010") === 1.0)

      assert(cols(1).getOptionalFactor.getLevelCounts.get("WN") === 28.0)

      assert(cols(2).getOptionalFactor.getLevelCounts.get("ISP") === 12.0)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("IAD") === 2.0)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("IND") === 17.0)

      assert(cols(3).getOptionalFactor.getLevelCounts.get("MCO") === 3.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("TPA") === 3.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("JAX") === 1.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("LAS") === 3.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("BWI") === 10.0)

      assert(cols(5).getOptionalFactor.getLevelCounts.get("0") === 9.0)
      assert(cols(4).getOptionalFactor.getLevelCounts.get("3") === 1.0)

      val ddf2 = manager.sql2ddf("select * from ddf://adatao/airlineWithNA")
      //    ddf2.getRepresentationHandler.remove(classOf[RDD[_]], classOf[TablePartition])

      val schemaHandler2 = ddf2.getSchemaHandler
      Array(0, 8, 16, 17, 24, 25).foreach {
        idx => schemaHandler2.setAsFactor(idx)
      }
      schemaHandler2.computeFactorLevelsAndLevelCounts()

      val cols2 = Array(0, 8, 16, 17, 24, 25).map {
        idx => schemaHandler2.getColumn(schemaHandler2.getColumnName(idx))
      }

      assert(cols2(0).getOptionalFactor.getLevelCounts.get("2008") === 28.0)
      assert(cols2(0).getOptionalFactor.getLevelCounts.get("2010") === 1.0)

      assert(cols2(1).getOptionalFactor.getLevelCounts.get("WN") === 28.0)

      assert(cols2(2).getOptionalFactor.getLevelCounts.get("ISP") === 12.0)
      assert(cols2(2).getOptionalFactor.getLevelCounts.get("IAD") === 2.0)
      assert(cols2(2).getOptionalFactor.getLevelCounts.get("IND") === 17.0)

      assert(cols2(3).getOptionalFactor.getLevelCounts.get("MCO") === 3.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("TPA") === 3.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("JAX") === 1.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("LAS") === 3.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("BWI") === 10.0)

      assert(cols2(5).getOptionalFactor.getLevelCounts.get("0") === 9.0)
      assert(cols2(4).getOptionalFactor.getLevelCounts.get("3") === 1.0)
    }
  }

  def ddfWithViewHandler(implicit l: Loader): Unit = {
    val airlineDDF = l.loadAirlineDDF()

    it should "project after remove columns " in {
      val ddf = airlineDDF
      val columns: java.util.List[String] = new java.util.ArrayList()
      columns.add("newYear")
      columns.add("newMonth")
      columns.add("newDeptime")

      val transformList = new util.ArrayList[String]()
      transformList.add("newYear=year")
      transformList.add("newMonth=month")
      transformList.add("newDeptime=deptime")
      val transformDDF = ddf.transform(transformList)
      transformDDF.getNumColumns should be (32)

      val newddf1: DDF = transformDDF.VIEWS.removeColumn("newYear")
      newddf1.getNumColumns should be(31)
      val newddf2: DDF = transformDDF.VIEWS.removeColumns("newYear", "newDeptime")
      newddf2.getNumColumns should be(30)
      val newddf3: DDF = transformDDF.VIEWS.removeColumns(columns)
      newddf3.getNumColumns should be(29)
    }
  }
}
