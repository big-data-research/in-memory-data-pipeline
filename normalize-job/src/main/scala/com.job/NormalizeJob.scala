package com.job

import java.io.{DataOutputStream, InputStreamReader, BufferedReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkConf, SparkContext}
import api.{SparkJobInvalid, SparkJobValid, SparkJobValidation, SparkJob}
import com.typesafe.config.Config


import scala.collection.mutable.ListBuffer

/**
 * Created by raduc on 03/11/14.
 */
object NormalizeJob {
  final val COLUMN_SEPARATOR = ","
  final val excludeColumns = List(2, 4, 18, 19)
  final val keepColumns = Range(0, numberOfColumns).diff(excludeColumns).toArray
  final val numberOfColumns = 23
}

class NormalizeJob extends SparkJob with Serializable{

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {

    import NormalizeJob._

    val inputFolder = jobConfig.getString("inputFolder")
    val outputFolder = jobConfig.getString("outputFolder")
    val schemeFile = jobConfig.getString("schemeFile")
    val countryFile = jobConfig.getString("countryFile")

    val fs = getFileSystem(schemeFile)
    fs.delete(new Path(outputFolder), true)

    val columns = sc.textFile(schemeFile).first().split(",")
    val usedColumns = getSelectedValues(columns, keepColumns).mkString(",")

    val output = fs.create(new Path(outputFolder + Path.SEPARATOR + "scheme.txt"))
    output.writeBytes(usedColumns)
    output.close()

    val countries = sc.textFile(countryFile).map{ line =>
      val values = line.split(COLUMN_SEPARATOR)
      (values(2), values(1))
    }

    val columnsIdsBroadcast = sc.broadcast(keepColumns)

    val filesRdd = sc.textFile(inputFolder, 12)

    val filteredRdd = filesRdd.filter{ line =>
      line.split(COLUMN_SEPARATOR).size == numberOfColumns
    }

    val normalizedRdd = filteredRdd.map { line =>
      val values = line.split(COLUMN_SEPARATOR)
      val selectedValues = getSelectedValues(values, columnsIdsBroadcast.value)
      ((selectedValues(4)), selectedValues)
    }

//    val repRdd = normalizedRdd.repartition(24)

    import org.apache.spark.SparkContext._
    val finalRdd = normalizedRdd.join(countries).map   {
      case (k: String, (columns: ListBuffer[String], charCode: String)) => {
        columns(4) = charCode
        columns
      }
    }

    val linesRdd = finalRdd.map { array => array.mkString(",")}

    linesRdd.cache()
    linesRdd.saveAsTextFile(outputFolder + Path.SEPARATOR + "data")

    finalRdd.count
  }

  def getSelectedValues(values: Array[String], columnsIds: Array[Int]) = {
    var newValues = ListBuffer[String]()
    columnsIds.foreach { columnId =>
      newValues += values(columnId)
    }
    newValues
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    if(!config.hasPath("inputFolder")) SparkJobInvalid("The \"inputFolder\" parameter is missing.")
    if(!config.hasPath("schemeFile")) SparkJobInvalid("The \"schemeFile\" parameter is missing.")
    if(!config.hasPath("countryFile")) SparkJobInvalid("The \"countryFile\" parameter is missing.")
    if(!config.hasPath("outputFolder")) SparkJobInvalid("The \"outputFolder\" parameter is missing.")
    SparkJobValid()
  }

  def getFileSystem(path: String) : FileSystem = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", path)
    FileSystem.get(conf)
  }
}