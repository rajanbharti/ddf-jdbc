package io.ddf.jdbc.etl

import java.util.Collections

import com.google.common.collect.Lists
import io.ddf.DDF
import io.ddf.analytics.Summary
import io.ddf.etl.IHandleMissingData.{Axis, NAChecking}
import io.ddf.etl.Types.JoinType
import io.ddf.etl.{TransformationHandler => DDFT}
import io.ddf.jdbc.BetterList._
import io.ddf.jdbc.content.{Representations, SqlArrayResult}
import io.ddf.jdbc.{BaseBehaviors, Loader}
import io.ddf.types.AggregateTypes.AggregateFunction
import org.scalatest.FlatSpec

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait ETLBehaviors extends BaseBehaviors {
  this: FlatSpec =>

  def ddfWithBasicJoinSupport(implicit l: Loader): Unit = {
    val airlineDDF = l.loadAirlineDDF()
    val yearNamesDDF = l.loadYearNamesDDF()

    it should "inner join tables" in {
      val ddf: DDF = airlineDDF
      val ddf2: DDF = yearNamesDDF
      val joinedDDF = ddf.join(ddf2, null, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
      val rep = joinedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
      val collection = rep.result
      collection.foreach(i => println("[" + i.mkString(",") + "]"))
      val list = seqAsJavaList(joinedDDF.sql("SELECT DISTINCT YEAR FROM " + joinedDDF.getUri, "Error").getRows)
      list.size should be(2) // only 2 values i.e 2008 and 2010 have values in both tables
      rep.schema.getNumColumns should be(31) //29 columns in first plus 2 in second
      val colNames = asBetterList(joinedDDF.getSchema.getColumnNames)
      colNames.containsIgnoreCase("YEAR") should be(true)
      //check if the names from second ddf have been added to the schema
      colNames.containsIgnoreCase("R_NAME") should be(true)

    }


    it should "left outer join tables" in {
      val ddf: DDF = airlineDDF
      val ddf2: DDF = yearNamesDDF
      val joinedDDF = ddf.join(ddf2, JoinType.LEFT, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
      val rep = joinedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
      val collection = rep.result
      collection.foreach(i => println("[" + i.mkString(",") + "]"))
      val list = seqAsJavaList(joinedDDF.sql("SELECT DISTINCT YEAR FROM " + joinedDDF.getUri, "Error").getRows)
      list.size should be(3) // 3 distinct values in airline years 2008,2009,2010
      val first = list.get(0)
      rep.schema.getNumColumns should be(31) //29 columns in first plus 2 in second
      val colNames = asBetterList(joinedDDF.getSchema.getColumnNames)
      colNames.containsIgnoreCase("YEAR") should be(true)
      //check if the names from second ddf have been added to the schema
      colNames.containsIgnoreCase("R_NAME") should be(true)
    }
  }

  def ddfWithSemiJoinSupport(implicit l: Loader): Unit = {

    val airlineDDF = l.loadAirlineDDF()
    val yearNamesDDF = l.loadYearNamesDDF()

    it should "left semi join tables" in {
      val ddf: DDF = airlineDDF
      val ddf2: DDF = yearNamesDDF
      val joinedDDF = ddf.join(ddf2, JoinType.LEFTSEMI, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
      val rep = joinedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
      val collection = rep.result
      collection.foreach(i => println("[" + i.mkString(",") + "]"))
      val list = seqAsJavaList(collection)
      list.size should be(2) // only 2 values i.e 2008 and 2010 have values in both tables
      val first = list.get(0)
      rep.schema.getNumColumns should be(29) //only left columns should be fetched
      val colNames = asBetterList(joinedDDF.getSchema.getColumnNames)
      colNames.containsIgnoreCase("YEAR") should be(true)
      //check if the names from second ddf have been added to the schema
      colNames.containsIgnoreCase("R_NAME") should be(false)

    }
  }

  def ddfWithFullOuterJoinSupport(implicit l: Loader): Unit = {

    val airlineDDF = l.loadAirlineDDF()
    val yearNamesDDF = l.loadYearNamesDDF()

    it should "full outer join tables" in {
      val ddf: DDF = airlineDDF
      val ddf2: DDF = yearNamesDDF
      val joinedDDF = ddf.join(ddf2, JoinType.FULL, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
      val rep = joinedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
      val collection = rep.result
      val list = seqAsJavaList(joinedDDF.sql("SELECT DISTINCT YEAR FROM " + joinedDDF.getUri, "Error").getRows)
      list.size should be(4)
      val first = list.get(0)
      rep.schema.getNumColumns should be(31) //29 columns in first plus 2 in second
      val colNames = asBetterList(joinedDDF.getSchema.getColumnNames)
      colNames.containsIgnoreCase("YEAR") should be(true)
      //check if the names from second ddf have been added to the schema
      colNames.containsIgnoreCase("R_NAME") should be(true)

    }
  }

  def ddfWithRightOuterJoinSupport(implicit l: Loader): Unit = {

    val airlineDDF = l.loadAirlineDDF()
    val yearNamesDDF = l.loadYearNamesDDF()

    it should "right outer join tables" in {
      val ddf: DDF = airlineDDF
      val ddf2: DDF = yearNamesDDF
      val joinedDDF = ddf.join(ddf2, JoinType.RIGHT, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
      val rep = joinedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
      val collection = rep.result
      collection.foreach(i => println("[" + i.mkString(",") + "]"))
      val list = seqAsJavaList(joinedDDF.sql("SELECT DISTINCT YEAR FROM " + joinedDDF.getUri, "Error").getRows)
      list.size should be(3) // 3 distinct values in null ,2008,2010
      val first = list.get(0)
      rep.schema.getNumColumns should be(31) //29 columns in first plus 2 in second
      val colNames = asBetterList(joinedDDF.getSchema.getColumnNames)
      colNames.containsIgnoreCase("YEAR") should be(true)
      //check if the names from second ddf have been added to the schema
      colNames.containsIgnoreCase("R_NAME") should be(true)
    }
  }

  def ddfWithMissingDataDropSupport(implicit l: Loader): Unit = {
    val missingData = l.loadAirlineNADDF()

    it should "drop all rows with NA values" in {
      val result = missingData.dropNA()
      result.getNumRows should be(9)
    }

    it should "keep all the rows" in {
      val result = missingData.getMissingDataHandler.dropNA(Axis.ROW, NAChecking.ALL, 0, null)
      result.getNumRows should be(31)
    }

    it should "keep all the rows when drop threshold is high" in {
      val result = missingData.getMissingDataHandler.dropNA(Axis.ROW, NAChecking.ALL, 10, null)
      result.getNumRows should be(31)
    }


    it should "drop all columns with NA values" in {
      val result = missingData.dropNA(Axis.COLUMN)
      result.getNumColumns should be(22)
    }

    it should "drop all columns with NA values with load table" in {
      val missingData = l.loadAirlineNADDF()
      val result = missingData.dropNA(Axis.COLUMN)
      result.getNumColumns should be(22)
    }

    it should "keep all the columns" in {
      val result = missingData.getMissingDataHandler.dropNA(Axis.COLUMN, NAChecking.ALL, 0, null)
      result.getNumColumns should be(29)
    }

    it should "keep most(24) columns when drop threshold is high(20)" in {
      val result = missingData.getMissingDataHandler.dropNA(Axis.COLUMN, NAChecking.ALL, 20, null)
      result.getNumColumns should be(24)
    }
  }

  def ddfWithMissingDataFillSupport(implicit l: Loader): Unit = {

    it should "fill by value" in {
      val ddf = l.loadAirlineNADDF()
      val ddf1: DDF = ddf.VIEWS.project(Lists.newArrayList("year", "lateaircraftdelay"))
      val filledDDF: DDF = ddf1.fillNA("0")
      val annualDelay = filledDDF.aggregate("year, sum(lateaircraftdelay)").get("2008")(0)
      annualDelay should be(282.0 +- 0.1)
    }

    it should "fill by dictionary" in {
      val ddf = l.loadAirlineNADDF()
      val ddf1: DDF = ddf.VIEWS.project(Lists.newArrayList("year", "securitydelay", "lateaircraftdelay"))
      val dict: Map[String, String] = Map("year" -> "2000", "securitydelay" -> "0", "lateaircraftdelay" -> "1")
      val filledDDF = ddf1.getMissingDataHandler.fillNA(null, null, 0, null, dict, null)
      val annualDelay = filledDDF.aggregate("year, sum(lateaircraftdelay)").get("2008")(0)
      annualDelay should be(302.0 +- 1)
    }

    it should "fill by aggregate function" in {
      val ddf = l.loadAirlineNADDF()
      val ddf1: DDF = ddf.VIEWS.project(Lists.newArrayList("year", "securitydelay", "lateaircraftdelay"))
      val result = ddf1.getMissingDataHandler.fillNA(null, null, 0, AggregateFunction.MEAN, null, null)
      result should not be (null)
    }
  }

  def ddfWithBasicTransformSupport(implicit l: Loader): Unit = {
    val ddf = l.loadAirlineDDF().sql2ddf("select year, month, deptime, arrtime, distance, arrdelay, depdelay from ddf://adatao/airline")

    it should "transform scale min max" in {
      ddf.getSummary foreach println _

      val newddf0: DDF = ddf.Transform.transformScaleMinMax

      val summaryArr: Array[Summary] = newddf0.getSummary
      println("result summary is" + summaryArr(0))
      summaryArr(0).min should be < 1.0
      summaryArr(0).max should be(1.0)
    }

    it should "transform scale standard" in {
      val newDDF: DDF = ddf.Transform.transformScaleStandard()
      newDDF.getNumRows should be(31)
      newDDF.getSummary.length should be(7)
    }


    it should "test transform udf" in {
      val newddf = ddf.Transform.transformUDF("dist= round(distance/2, 2)")
      newddf.getNumRows should be(31)
      newddf.getNumColumns should be(8)
      newddf.getColumnName(7).toLowerCase should be("dist")

      // PE-1466
      // XXX this will fail as the auto created column name from postgres will be ?column?
      // and is not a valid column name required by our Schema
      // val newddf2 = newddf.Transform.transformUDF("arrtime-deptime")
      val newddf2 = newddf.Transform.transformUDF("triptime=arrtime-deptime")
      newddf2.getNumRows should be(31)
      newddf2.getNumColumns should be(9)

      val cols = Lists.newArrayList("distance", "arrtime", "deptime", "arrdelay")
      val newddf3 = newddf2.Transform.transformUDF("speed = distance/(arrtime-deptime)", cols)
      newddf3.getNumRows should be(31)
      newddf3.getNumColumns should be(5)
      newddf3.getColumnName(4).toLowerCase should be("speed")

      //      val lcols = Lists.newArrayList("distance", "arrtime", "deptime")
      //      val s0: String = "new_col = if(arrdelay=15,1,0)"
      //      val s1: String = "new_col = if(arrdelay=15,1,0),v ~ (arrtime-deptime),distance/(arrtime-deptime)"
      //      val s2: String = "arr_delayed=if(arrdelay=\"yes\",1,0)"
      //      val s3: String = "origin_sfo = case origin when \'SFO\' then 1 else 0 end "
      //      val res1 = "(if(arrdelay=15,1,0)) as new_col,((arrtime-deptime)) as v,(distance/(arrtime-deptime))"
      //      val res2 = "(if(arrdelay=\"yes\",1,0)) as arr_delayed"
      //      val res3 = "(case origin when \'SFO\' then 1 else 0 end) as origin_sfo"
      //      DDFT.RToSqlUdf(s1) should be(res1)
      //      DDFT.RToSqlUdf(s2) should be(res2)
      //      DDFT.RToSqlUdf(s3) should be(res3)

    }
  }

  def ddfWithSqlHandler(implicit l: Loader): Unit = {


    it should "create table and load data from file" in {
      val ddf = l.loadAirlineDDF()
      ddf.getColumnNames should have size 29

      //MetaDataHandler
      ddf.getNumRows should be(31)

      //StatisticsComputer
      val summaries = ddf.getSummary
      summaries.head.max() should be(2010)

      val randomSummary = summaries(6)
      randomSummary.variance() should be(260747.76 +- 1.0)
    }

    it should "run a simple sql command" in {
      val ddf = l.loadAirlineDDF()
      val ddf1 = ddf.sql2ddf("select Year,Month from ddf://adatao/airline")
      val rep = ddf1.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
      val collection = rep.result
      val list = collection.asJava
      println(list)
      rep.schema.getNumColumns should be(2)
      list.head(0).toString should startWith("20")
    }

    it should "run a sql command with where" in {
      val ddf = l.loadAirlineDDF()
      val ddf1 = ddf.sql2ddf("select Year,Month from ddf://adatao/airline where Year > 2008 AND Month > 1")
      val rep = ddf1.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
      val collection = rep.result
      val list = collection.asJava
      println(list)
      list.size should be(1)
      rep.schema.getNumColumns should be(2)
      list.head(0) should be(2010)
    }

  }
}
