package com.job

import akka.actor.ActorSystem
import client.SparkJobRestClient
import org.apache.spark.{SparkContext, SparkConf}
import responses.JobStates
import spray.http.DateTime

/**
 * Created by raduchilom on 5/4/15.
 */

object ExecuteNormalize extends App {
  implicit val system = ActorSystem()
  val contextName = "transformDataContext"

  try {
    val sjrc = new SparkJobRestClient("http://localhost:8097")

    val context = sjrc.createContext(contextName, Map("jars" -> "/home/ubuntu/normalize-job.jar", "spark.executor.memory" -> "4g"))
    println(context)

    val inputFolder = "\"tachyon://localhost:19998/user/ubuntu/downloaded_data/data/\""
    val schemeFile = "\"tachyon://localhost:19998/user/ubuntu/downloaded_data/scheme/scheme.txt\""
    val countryFile = "\"hdfs://10.0.2.110/user/ubuntu/countries/country.csv\""
    val outputFolder = "\"tachyon://10.0.2.85:19998/user/ubuntu/normalized_data\""

//    val runningClass = "com.job.NormalizeJob"
    val runningClass = "com.job.NormalizeBroadcastJob"
    val job = sjrc.runJob(runningClass, contextName,
      Map("inputFolder" -> inputFolder,
        "schemeFile" -> schemeFile,
        "countryFile" -> countryFile,
        "outputFolder" -> outputFolder
      ))
    println(job)
    val time1 = DateTime.now


    var jobFinal = sjrc.getJob(job.jobId, job.contextName)
    while (jobFinal.status.equals(JobStates.RUNNING.toString())) {
      Thread.sleep(1000)
      jobFinal = sjrc.getJob(job.jobId, job.contextName)
    }
    println(jobFinal)
    val time = DateTime.now.-(time1.clicks).clicks
    println("Duration: " + time)



//    sjrc.deleteContext(contextName)
  } catch {
    case e:Exception => {
      e.printStackTrace()
    }
  }

  system.shutdown()
}