import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger,Level}

object ReportManager  {
  val logger = Logger.getLogger(getClass.getName)

  // сюда добавлять отчёты - поддерживающие интерфейс (trait) Report0
  val reports = Seq(
    new ReportFarma,
    new ReportInitFarma // псевдоотчёт для инициализации
  ).sortBy(_.shortName)
  logger.debug(s"ReportManager has ${reports.size} reports registred.")

  def showReportList(): Unit = {
    logger.debug("Showing report list")
    println("Возможные отчёты:")
    for (report <- reports) {
      println(report.shortName + " - " + report.shortDescr)
    }
    println()
  }

  def runReport(reportName: String, params: Map[String,String])(implicit conf:SparkConf): Unit =
  {
    reports.find(_.shortName==reportName) match {
      case Some(report) =>

        implicit val spark: SparkSession = SparkSession.builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()

        if (report.check(params)) {
          report.run(params)
        } else {
          println("К сожалению, предварительная проверка не прошла. Проверьте параметры: "+params.mkString(", "))
          logger.error("Fail to validate input parameters or source data")
        }

        spark.stop()

      case _ => // TODO: throw exception?
        println(s"Error! Report $reportName is unknown! Нет такого отчёта!")
        logger.error("No such report: "+reportName)
        showReportList()
    }
  }

}
