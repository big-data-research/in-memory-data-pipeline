

import api.SparkJob
import org.apache.spark.sql.parquet.SparkParquetUtility._
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import api.SparkJobValidation
import api.SparkJobValid
import api.SparkJobInvalid
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import JobConfigurationKeys._
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.InputStream
import org.apache.spark.sql.types._

class WriteParquetFiles extends SparkJob {

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlContext = new SQLContext(sc)
    val inputFolder = jobConfig.getString(INPUT_FOLDER_PATH.toString())
    val outputFolder = jobConfig.getString(OUTPUT.toString())
    val outputNamenode = jobConfig.getString(OUTPUT_NAMENODE.toString())
    val inputConfiguration = getHadoopConf(inputFolder)
    
    val schema = readSchema(inputConfiguration, s"$inputFolder/scheme.txt")
    val valuesRowRdd = readRows(sc, s"$inputFolder/data")

    val transactionsSchemaRDD = sqlContext.createDataFrame(valuesRowRdd, schema)
    transactionsSchemaRDD.saveAsXPatternsParquet(outputNamenode, outputFolder)
  }
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    val keyIterator = values.iterator
    var key = ""
    var missingKey = false
    while (keyIterator.hasNext && missingKey == false) {
      key = keyIterator.next().toString()
      missingKey = !config.hasPath(key)
    }

    if (missingKey)
      SparkJobInvalid(s"Missing key $key")
    else
      SparkJobValid()

  }

  def readSchema(sc: SparkContext, schemaFile: String) = {
    val schemaRDD = sc.textFile(schemaFile)
    val schemaFields: Array[StructField] = schemaRDD.flatMap(_ split (",")).map(fieldName => StructField(fieldName, StringType, true)).collect
    StructType(schemaFields)
  }

  def readSchema(configuration: Configuration, schemaFile: String) = {

    var content = ""
    var fs: FileSystem = null
    var is: InputStream = null
    try {
      val filePath = new Path(schemaFile)
      fs = FileSystem.newInstance(configuration)
      is = fs.open(filePath)

      val firstLine = scala.io.Source.fromInputStream(is).getLines().next()
      val schemaFields = firstLine.split(",").map(fieldName => StructField(fieldName, StringType, true))
      StructType(schemaFields)
    } finally {
      if(is != null) {
        is.close()
      }
      if (fs != null) {
        fs.close()
      }
    }

  }

  def readRows(sc: SparkContext, valuesFolder: String) = {
    val valuesRdd = sc.textFile(valuesFolder) map (_.split(","))
    valuesRdd map (Row.fromSeq(_))
  }

  def getHadoopConf(defaultFS: String) = {
    val configuration = new Configuration()
    configuration.set("fs.defaultFS", defaultFS)

    configuration
  }
}