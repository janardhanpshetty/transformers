package transformers.ml

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Created by jshetty on 8/13/16.
 */
trait SparkFunContext extends BeforeAndAfterAll{ self: Suite =>
  @transient var spark: SparkSession = _
  @transient var sc: SparkContext = _

  override def beforeAll() {
    spark = SparkSession.builder.master("local[8]").appName("Transformer UnitTests").getOrCreate()
    sc = spark.sparkContext
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }
}
