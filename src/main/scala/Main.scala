import org.apache.spark.SparkConf
import org.apache.log4j.{Logger,Level}

object Main extends App {

  org.apache.log4j.BasicConfigurator.configure()
  Logger.getRootLogger.setLevel(Level.INFO)
  val logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  implicit val conf = new SparkConf()
    .set("spark.sql.catalogImplementation", "hive")
    //.set("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
    .setMaster("local[*]")
    .setAppName("ofd-reports")

  if (args.length<=0) {
    println("Требуется параметр: кодовое имя отчёта.")
    println("Некоторые отчёты используют дополнительные параметры (формат: имя=знчение).")
    ReportManager.showReportList()
    logger.warn("No report specified")
  }
  else {
    // после имени отчёта могут идти его дополнительные параметы
    val params0 = if (args.length<=1) Map.empty[String,String]
    else (1 to args.length-1).map(args).map(_.split('=')).collect{case a if a.size==2=>a(0)->a(1)}.toMap

    // Параметр "hdfs", если не задан, инициализируется автоматически значением по-умолчанию
    val params = if (params0.contains("hdfs")) params0 else {
      val default = "/"
      logger.info("Using default HDFS: "+default)
      params0 + ("hdfs"->default)
    }

    ReportManager.runReport( args(0) , params )
  }

  // Для бустрой проверки:
  //ReportManager.runReport( "farma" ,
  //  Map.empty[String,String]+("hdfs"->"hdfs://172.17.0.4:9000/")+("month"->"2018-02") )
}
