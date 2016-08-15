package transformers.ml

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.FunSuite

/**
 * Created by jshetty on 8/14/16.
 */

case class HasherTestData(inputWordsArray: Array[String], wantedHashArray: Array[Long])

object HasherSuite extends FunSuite with SparkFunContext {
  def testHasher(t: Hasher, dataset: Dataset[_]): Unit = {
    t.transform(dataset)
      .select("hashArray", "wantedHashArray")
      .collect()
      .foreach { case Row(actualHashValue, wantedHashValue) =>
      assert(actualHashValue === wantedHashValue)
    }
  }
}

class HasherSuite extends FunSuite with SparkFunContext {

  import transformers.ml.HasherSuite._
  test("Hasher Test") {
    val hasher = new Hasher().setInputCol("inputWordsArray").setOutputCol("hashArray").setSeed(42)

    val dataset = spark.createDataFrame(Seq(HasherTestData(
      Array("a", "b", "abc"),
      Array(1293573533, 861554165, 292716463)
    ))
    )

    testHasher(hasher, dataset)
  }
}


