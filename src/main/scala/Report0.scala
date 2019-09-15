import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

// Шаблон для написания отчётоа
trait Report0 {

  def shortName : String                // кодовое имя отчёта
  def shortDescr: String                // короткое описание
  def longDescr : String = shortDescr   // длинное описание

  // обязательные параметры отчёта задаются в requiredParams
  case class ReportParam(name: String, descr: String) { override def toString: String = name }
  implicit def paramFromPair(p:(String,String)) = ReportParam.apply(p._1,p._2)
  val requiredParams : Seq[ReportParam]

  // Для предварительной проверки перед выполнением // do parameters and source data look fine?
  def check(param: Map[String,String] = Map.empty)(implicit spark: SparkSession) : Boolean = true

  // Выполняет отчёт // run report - may return the reference to output table
  def run(param: Map[String,String] = Map.empty)(implicit spark: SparkSession) : String

  // Генерирует текст для пользователя про все параметры запроса
  def paramsDescr: String = "Входные параметры:\n" +
    (Stream.from(1) zip requiredParams).map {
      case (num, ReportParam(name,descr)) => s"$num) $name - $descr"
    }.mkString(";\n")

  protected val logger = Logger.getLogger(getClass.getName)
}

