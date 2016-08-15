package transformers.ml

import org.scalatest.FunSuite

/**
 * Created by jshetty on 8/13/16.
 */
class CapitalizerSuite extends FunSuite with SparkFunContext{
  test("Capitalizer Test"){

    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("label", "words")

    val capitalizer = new Capitalizer().setInputCol("words").setOutputCol("capital").transform(wordDataFrame)

    val expected = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark"), Array("Hi", "I", "Heard", "About", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes"),  Array("I", "Wish", "Java", "Could", "Use", "Case", "Classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"), Array("Logistic", "Regression", "Models", "Are", "Neat"))
    )).toDF("label", "words","capital")

    assert(capitalizer.collect().deep == expected.collect().deep)

  }

}
