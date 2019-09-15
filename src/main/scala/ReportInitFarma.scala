import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.joda.time.DateTime

import scala.util.Try
class ReportInitFarma extends Report0 {
  override def shortName: String = "farmainit"

  override def shortDescr: String = "Псевдоотчёт инициализирующий таблицы для отчёта 'farma'"

  override val requiredParams: Seq[ReportParam] = Seq( "hdfs"  -> "HDFS URI (корень с '/' на конце)" )

  override def run(param: Map[String, String])(implicit spark: SparkSession): String = {
    // получаем значения параметров запроса
    val t = Try { param.get("hdfs").get }
    if (t.isFailure) {
      logger.error("Cannot get required report parameters!")
      return new String; // TODO: throw exception here
    }
    val hdfs = t.get

    def load(table:String) = {
      val fileName = table+".csv"
      val options = Map( "encoding" -> "UTF-8", "header" -> "true"  )
      val df = spark.read.format("csv")
        .options(options)
        .option("inferSchema","true") // - it reads inn as Int32
        .load(fileName)
      df.show()
      df.printSchema()
      val schema = df.schema
      if (schema.fieldNames.contains("inn"))
      {
        // force inn to be of String
        val newSchema = StructType( schema.map { f=> if (f.name=="inn") StructField("inn",StringType) else f } )
        val df = spark.read.format("csv")
          .options(options)
          .schema(newSchema)
          .load(fileName)
        df.show()
        print("FIXED:")
        df.printSchema()
        df
      }
      else
        df
    }

    val srcdata =
      Seq( "inn_info.farma", "misc.adm_area", "receipts" )
        //Seq( "receipts" )
        .map { f => f -> load(f) }

    // Note: fo_id пуст для region_id=99

    srcdata.map { _ match {
      // to Hive
      case (name,df) if name!="receipts" =>

        logger.info("Going to save into Hive: "+name)
        df.printSchema()

        val splitted = name.split('.')
        if (splitted.length>1)
        {
          logger.info(s"Database '${splitted(0)}' will be created if it doesn't exist.")
          spark.sql(s"CREATE DATABASE IF NOT EXISTS ${splitted(0)}") // injection horror
        }

        df.write
          .mode(Overwrite)
          .format("parquet")
          .saveAsTable( name )

      // to HDFS
      case (name,df) if name=="receipts" =>
        logger.info("Using HDFS at "+hdfs)

        // fill a dump data range
        val pattern = "yyyy-MM-dd"
        // val dataFormatter = DateTimeFormat.forPattern(pattern)
        val start = DateTime.parse("2018-01-01")
        val end   = DateTime.parse("2018-04-10")
        val dates = Iterator.iterate(start) { _.plusDays(1) }.takeWhile(_.isBefore(end))

        for ( date <- dates.map(_.toString(pattern)) )
        {
          val target = hdfs+"user/big/receipt/"+date
          logger.info(s"Saveing from $name to $target")
          df.write
            .mode(Overwrite)
            .partitionBy("region_id") // "shop_id",
            .parquet(target)
        }

    } }

    logger.info(s"Database 'reports' will be created if it doesn't exist.")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS reports")

    logger.info("Success. Инициализация завершена.")
    new String
  }
}
