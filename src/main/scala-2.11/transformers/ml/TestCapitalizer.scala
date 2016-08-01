package transformers.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

/**
 * Created by jshetty on 7/31/16.
 */
object TestCapitalizer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[8]").appName("Transformer").getOrCreate()
    val wordDataFrame = spark.createDataFrame(Seq((0, "spark"), (1, "wish"), (2, "regression"))).toDF("label", "words")

    val capitalizer = new Capitalizer().setInputCol("words").setOutputCol("capital")
    val capitalizerDF = capitalizer.transform(wordDataFrame)
    capitalizerDF.show()

  }
}
