package transformers.ml

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{IntParam, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by jshetty on 8/4/16.
 */
class StopWordsShingler(override val uid: String) extends UnaryTransformer[Seq[String], Seq[String], StopWordsShingler] {
  def this() = this(Identifiable.randomUID("stopWordsShingler"))

  val n: IntParam = new IntParam(this, "n", "number elements per n-stopWordsShingler (>=1)", ParamValidators.gtEq(1))
  def setN(value: Int): this.type = set(n, value)
  def getN: Int = $(n)
  setDefault(n -> 2)

  val supportedLanguages = Set("danish", "dutch", "english", "finnish", "french", "german",
    "hungarian", "italian", "norwegian", "portuguese", "russian", "spanish", "swedish", "turkish")

  // Load the stopwords
  def loadDefaultStopWords(language: String): Array[String] = {
    require(supportedLanguages.contains(language), s"$language is not in the supported language list: ${supportedLanguages.mkString(", ")}.")
    val is = getClass.getResourceAsStream(s"/org/apache/spark/ml/feature/stopwords/$language.txt")
    scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toArray
  }
  val stopWords: Array[String] = loadDefaultStopWords("english")

  override protected def createTransformFunc: (Seq[String]) => Seq[String] = { words =>
    val stopWordsSet = stopWords.toSet
    val resultArray = new ArrayBuffer[String]()
    for (i <- 0 to words.length - 1) {
      // Convert to lowercase
      if (stopWordsSet.contains(words(i).toLowerCase())) {
        // Make it lowercase for hashing
        resultArray += words.slice(i, i + $(n)).mkString(" ").toLowerCase()
      }
    }
    resultArray.toSeq
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, false)

}