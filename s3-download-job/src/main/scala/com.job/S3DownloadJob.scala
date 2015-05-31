package com.job

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import api.{SparkJobInvalid, SparkJobValid, SparkJobValidation, SparkJob}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.{Success, Failure, Try}

/**
 * Created by raduc on 03/11/14.
 */
class S3DownloadJob extends SparkJob {

  val log = LoggerFactory.getLogger(getClass)

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val bucketName = jobConfig.getString("s3.bucket")

    val numPartitions = jobConfig.getInt("num.partitions")
    val outputFolder = jobConfig.getString("fs.output")

//    slow for a large number of files
//    val fileList = S3Utils.getFiles(bucketName)
//    val files = sc.parallelize(fileList, numPartitions)

    val filesRdd = S3Utils.getFilesDistributed(bucketName, sc, numPartitions)
//    known bug in spark 1.1.0
//    filesRdd.partitions
    val files = filesRdd.repartition(numPartitions)

    log.info(s"Number of partitions: ${files.partitions.length}")

      val results = files.mapPartitions{ iterator =>

      val listBuffer = ListBuffer[(Try[Any], String)]()
      val s3Client = S3Utils.getS3Client()

      while(iterator.hasNext) {
        val tuple = iterator.next()
        listBuffer += S3Utils.downloadFile(bucketName, tuple._2, outputFolder, s3Client)
      }

      listBuffer.iterator
    }

    log.info("Listing the file with errors:")
    val errorFiles = results.filter { x =>
      x._1 match {
        case Success(v) => false
        case Failure(e) => true
      }
    }.collect()
    log.warn(s"There were ${errorFiles.size} files with error")
    errorFiles.foreach(t => log.error("", t._1.get))

    if(errorFiles.size == 0){
      s"Number of failed files: ${errorFiles.size}"
    } else {
      errorFiles
    }

  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
        if(config.hasPath("s3.bucket")) SparkJobInvalid("The \"s3.bucket\" parameter is missing.")

        if(config.hasPath("fs.output")) SparkJobInvalid("The \"fs.output\" parameter is missing.")
        if(config.hasPath("num.partitions")) SparkJobInvalid("The \"num.partitions\" parameter is missing.")

    SparkJobValid()
  }

}