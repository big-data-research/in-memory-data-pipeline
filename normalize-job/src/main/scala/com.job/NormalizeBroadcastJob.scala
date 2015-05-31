package com.job

/**
 * Created by raduchilom on 5/16/15.
 */

import java.io.{DataOutputStream, InputStreamReader, BufferedReader}

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkConf, SparkContext}
import api.{SparkJobInvalid, SparkJobValid, SparkJobValidation, SparkJob}


import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
 * Created by raduc on 03/11/14.
 */
object NormalizeBroadcastJob {
  final val COLUMN_SEPARATOR = ","
  final val excludeColumns = List(2, 4, 18, 19)
  final val keepColumns = Range(0, numberOfColumns).diff(excludeColumns).toArray
  final val numberOfColumns = 23
}

class NormalizeBroadcastJob extends SparkJob with Serializable{

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {

    import NormalizeJob._

    val inputFolder = jobConfig.getString("inputFolder")
    val outputFolder = jobConfig.getString("outputFolder")
    val schemeFile = jobConfig.getString("schemeFile")
    val countryFile = jobConfig.getString("countryFile")

//    val fs = getFileSystem(outputFolder)
//    fs.delete(new Path(outputFolder), true)

    val columns = sc.textFile(schemeFile).first().split(",")
    val usedColumns = getSelectedValues(columns, keepColumns).mkString(",")

    sc.parallelize(List(usedColumns),1).map{ columns =>
      val fileSystem = getFileSystem(outputFolder)
      fileSystem.delete(new Path(outputFolder), true)
      val output = fileSystem.create(new Path(outputFolder + Path.SEPARATOR + "scheme.txt"), true)
      output.writeBytes(usedColumns)
      output.close()
    }.count()

    import org.apache.spark.SparkContext._
    val countries = sc.textFile(countryFile).map{ line =>
      val values = line.split(COLUMN_SEPARATOR)
      (values(2), values(1))
    }.collectAsMap()
    val countriesBroadcast = sc.broadcast(countries)

    val columnsIdsBroadcast = sc.broadcast(keepColumns)

    val filesRdd = sc.textFile(inputFolder)
    val filteredRdd = filesRdd.filter{ line =>
      line.split(COLUMN_SEPARATOR).size == numberOfColumns
    }

    val normalizedRdd = filteredRdd.map { line =>
      val values = line.split(COLUMN_SEPARATOR)
      val selectedValues = getSelectedValues(values, columnsIdsBroadcast.value)
      selectedValues
    }

    val finalRdd = normalizedRdd.flatMap { array =>
      countriesBroadcast.value.get(array(4)) match {
        case None => Nil
        case Some(value) => {
          array(4) = value
          List(array.mkString(","))
        }
      }
    }

    finalRdd.cache()
    finalRdd.saveAsTextFile(outputFolder + Path.SEPARATOR + "data")

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
    conf.set("fs.tachyon.impl","tachyon.hadoop.TFS")
    conf.set("fs.defaultFS", path)
    FileSystem.get(conf)
  }

}