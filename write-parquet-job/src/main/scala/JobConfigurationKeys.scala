

object JobConfigurationKeys extends Enumeration {
  type JobConfigurationKeys = String
  
  //IO
  val INPUT_FOLDER_PATH = Value("input")
  val OUTPUT = Value("output") 
  val OUTPUT_NAMENODE = Value("output-namenode")
}