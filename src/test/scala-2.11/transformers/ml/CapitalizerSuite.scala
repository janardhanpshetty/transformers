package transformers.ml

import org.apache.spark.sql.{Row, Dataset}
import org.scalatest.FunSuite

/**
 * Created by jshetty on 8/13/16.
 */
case class CapitalizerTestData(inputWordsArray: Array[String], wantedWordsArray: Array[String])

class CapitalizerSuite extends FunSuite with SparkFunContext{
  import transformers.ml.CapitalizerSuite._

  test("Capitalizer Test"){

    val capitalizer = new Capitalizer().setInputCol("inputWordsArray").setOutputCol("capital")

    val dataset = spark.createDataFrame(Seq(
      CapitalizerTestData(
        Array("hi", "I", "heard", "about", "spark"),
        Array("Hi", "I", "Heard", "About", "Spark")
      )
    )
    )
    testCapitalizer(capitalizer, dataset)
  }
}

object CapitalizerSuite extends FunSuite with SparkFunContext{

  def testCapitalizer(t: Capitalizer, dataset: Dataset[_]): Unit = {
    t.transform(dataset)
      .select("capital", "wantedWordsArray")
      .collect()
      .foreach { case Row(actualWord, wantedWord) =>  assert(actualWord === wantedWord) }
  }
}
