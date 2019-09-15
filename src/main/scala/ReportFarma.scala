import java.net.URI
import scala.util.Try
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode.Overwrite

// Отчёт-пример
class ReportFarma extends Report0 {

  override def shortName:  String = "farma"
  override def shortDescr: String = "Месячный по рынку фармацептики"

  val requiredParams = Seq(
    "month" -> "месяц в формате YYYY-MM",
    "hdfs"  -> "HDFS URI (корень с '/' на конце)"
  )

  override def longDescr: String =
    "Месячный отчёт по рынку фармацептики.\n" + paramsDescr +
    "Исходные данные: 1) hdfs:/user/big/receipt/YYYY-MM-DD;\n" +
    "2) misc.adm_area; 3) inn_info.farma\n" +
    "Результат: reports.farmamonth_YYYY_MM"

  // Проверка валидности входных параметров и данных
  override def check(param: Map[String, String])(implicit spark: SparkSession): Boolean = {
    param.get("month").map(_.matches("""20(\d\d)-(\d\d)""")).getOrElse(false) &&
    param.isDefinedAt("hdfs")
    // TODO: check hdfs value; is it correct?
    // TODO: check if misc.adm_area and inn_info.farma is accessible
  }

  // Сам отчёт
  override def run(param: Map[String, String])(implicit spark: SparkSession): String = {
    import spark.sql
    import spark.implicits._

    // получаем значения параметров запроса
    val t = Try { (param.get("hdfs").get, param.get("month").get )}
    if (t.isFailure) {
      logger.error("Cannot get required report parameters!")
      return new String; // TODO: throw exception here
    }
    val (hdfs, month) = t.get

    logger.info(s"Starting $shortName report for Month=$month ...")

    val sc = spark.sparkContext
    val fs = FileSystem.get(new URI(hdfs),sc.hadoopConfiguration)
    val pathes = fs.listStatus(
      new Path(hdfs+"user/big/receipt/"),
      { path: Path => fs.isDirectory(path) && path.getName.matches(s"$month-[0123][0-9]") } // PathFilter
    ).map(_.getPath.toString)
    logger.info(s"Receipts located")
    if (pathes.isEmpty)
    {
      logger.warn("No data for month="+month)
      println($"Не найдены таблицы чеков за месяц $month. Он неверен?")
      return new String
    }

    // Если возникнет вопрос о производительности объединения по цепочке,
    // более оптимальная процедура приводится здесь:
    // https://stackoverflow.com/questions/37612622/spark-unionall-multiple-dataframes
    val dfReceipts = pathes.map( spark.read.parquet ).reduce(_ union _)
    dfReceipts.createOrReplaceGlobalTempView("receipts")
    logger.info(s"Receipts assembled to one DataFrame")

    // Этот запрос формирует отсновной источник данных для этого отчёта
    val globalTempDatabase = spark.conf.get("spark.sql.globalTempDatabase")
    val q =
      "SELECT inn, fo_id, region_id, region_name as region, shop_id, kkt_id, total_sum " +
        s"FROM $globalTempDatabase.receipts " +
        "JOIN inn_info.farma USING (inn) " +
        "LEFT JOIN misc.adm_area USING (region_id)"
    val df = sql(q)
    logger.debug("Got basic data source DataFrame")

    // Группировка по ИНН аптек для определения кол-ва магазинов и округов
    val dfInn = df.groupBy("inn").agg(
      countDistinct("shop_id").alias("count_shop"),
      countDistinct("fo_id").alias("count_fo")
    )
    // определение типов сетей (тип 0 = розница)
    val dfChains = dfInn.map {
      row =>
        row.getAs[String]("inn") -> (
          (
            row.getAs[Long]("count_shop"),
            row.getAs[Long]("count_fo")
          ) match {
            case (sh,fo) if sh>3 && fo>3  => 3
            case (sh,fo) if sh>3 && fo>1  => 2
            case (sh,fo) if sh>3 && fo<=1 => 1
            case _ => 0
          }
          )
    }.toDF("inn","chain_type")
    logger.debug("Chains classified")

    val dfChained = df.join(dfChains, Seq("inn"), "left")
    val result = dfChained.groupBy("region_id","chain_type").agg(
      concat_ws(", ",collect_set("region")).alias("region"), // expected only one region here
      countDistinct("shop_id").alias("count_shop"),
      countDistinct("kkt_id").alias("count_kkt"),
      sum("total_sum").alias("total")
    ).select("region","count_shop","count_kkt","total","chain_type") // упорядочивание и удаление region_id
    logger.debug(s"Ready to save results")

    println("result count="+result.count())
    result.printSchema()

    // сохранение результата
    val output = s"reports.${shortName}_${month.replace('-','_')}"   // таблица-результат
    result.write
      .mode(Overwrite)
      .format("parquet")
      //.partitionBy("region_id")
      .saveAsTable( output )

    logger.info(s"Report $shortName compleated. See the result: $output")
    output
  }

}
