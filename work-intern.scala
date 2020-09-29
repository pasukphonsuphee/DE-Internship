package th.co.truecorp.trueinsights.spark
import scala.util.{Failure, Success, Try}
import sys.process._
import org.apache.spark.sql.{SaveMode, SparkSession}
import spray.json._
import th.co.truecorp.trueinsights.spark.JsonProtocol._
import scala.collection.mutable.ListBuffer

      val domainNames = nullTable
        .select("domainname")
        .map(row => row.getAs[String]("domainname"))
        .collect()
        .toList
      var count = 0
      var index = 0
      val response = new ListBuffer[SimilarWeb]()
      while ((count < numOfLimit) && (index < domainNames.size)) {
        val domainName = domainNames(index)
        val categoryRankUrl =
          s"https://api.similarweb.com/v1/website/${domainName}/category-rank/category-rank?api_key={$apiKey}&format=json"
        val categoryRankReq = curl +" "+ categoryRankUrl //seq => :+ is append from seq
        val catResult = (categoryRankReq.!!)
        Try(catResult.parseJson.convertTo[CategoryRanks]) match {
          case Success(categoryRank) => {
            count = count + 1
            index = index + 1
            val splitCat = categoryRank.category.split("/").toList
            val cat = splitCat.head
            val subCat = splitCat.last
            val res =
              SimilarWeb(cat, subCat, domainName, categoryRank.rank, "helper")
            response += res
          }
          case Failure(_) => {
            index = index + 1
          }
        }
      }
      val result = response.toList.toDF()
      result.write
        .format("orc")
        .mode(SaveMode.Overwrite)
        .option("compression", "zlib")
        .save(savePath)
    } catch {
      case e: Exception =>
        throw handleException(e) // this will throw the exception from the handleException
    }
  }
  def handleException(e: Exception): Exception = {
    System.err.println("Handling Exception: " + e)
    new Exception(e)
  }
  def retry[T](n: Int)(attempt: => Try[T]): Try[T] = {
    attempt match {
      case x: Success[T] => x
      case Failure(_) if n > 1 => {
        println("Retry " + n)
        retry(n - 1)(attempt)
      }
      case f => f
    }
  }
}
