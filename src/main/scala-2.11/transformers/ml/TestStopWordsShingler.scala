package transformers.ml

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.SparkSession

/**
 * Created by jshetty on 8/3/16.
 */
object TestStopWordsShingler {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[8]").appName("TestStopWordsShingler").getOrCreate()
    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "A spokesperson for the Sudzo Corporation revealed today that students have shown it is good for people to buy Sudzo products")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val tokenized = tokenizer.transform(sentenceDataFrame)
    val shingler = new StopWordsShingler().setInputCol("words").setOutputCol("shingles").setN(3)
    shingler.transform(tokenized).select("words", "shingles").show(false)

  }

}



