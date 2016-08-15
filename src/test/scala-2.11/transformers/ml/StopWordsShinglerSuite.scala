package transformers.ml

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.FunSuite

/**
 * Created by jshetty on 8/14/16.
*/
case class StopWordsShinglerTestData(inputWords: Array[String], wantedShingles: Array[String])

object StopWordsShinglerSuite extends FunSuite with SparkFunContext{

  def testStopWordsShingler(t: StopWordsShingler, dataset: Dataset[_]): Unit = {
    t.transform(dataset)
      .select("shingles", "wantedShingles")
      .collect()
      .foreach { case Row(actualNShingles, wantedNShingles) =>
      assert(actualNShingles === wantedNShingles)
    }
  }
}

class StopWordsShinglerSuite extends FunSuite with SparkFunContext{

  import transformers.ml.StopWordsShinglerSuite._

  test("StopWordsShingler=3") {
    val shingler = new StopWordsShingler()
      .setInputCol("inputWords")
      .setOutputCol("shingles")
      .setN(3)

    val dataset = spark.createDataFrame(Seq(
      StopWordsShinglerTestData(
        Array("A", "spokesperson", "for", "the", "Sudzo", "Corporation", "revealed", "today", "that", "students", "have", "shown", "it", "is", "good", "for", "people", "to", "buy", "Sudzo", "products"),
        Array("a spokesperson for", "for the sudzo", "the sudzo corporation", "that students have", "have shown it", "it is good", "is good for", "for people to", "to buy sudzo")
      ))
    )

    testStopWordsShingler(shingler, dataset)
  }

}

