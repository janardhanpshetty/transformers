package transformers.ml

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{IntParam, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3.stringHash

/**
 * Created by jshetty on 8/9/16.
 */
class Hasher(override val uid: String) extends UnaryTransformer[Seq[String], Seq[Long], Hasher] {
  def this() = this(Identifiable.randomUID("hasher"))

  val seed: IntParam = new IntParam(this, "seed", "seed value", ParamValidators.gtEq(1))
  def setSeed(value: Int): this.type = set(seed, value)
  def getSeed: Int = $(seed)
  setDefault(seed -> 32)

  override protected def createTransformFunc: (Seq[String]) => Seq[Long] = { shingleArray =>
  val resultArray = new ArrayBuffer[Long]()
  for (i <- 0 to shingleArray.length - 1) {
    // Make it lowercase for hashing
    resultArray += math.abs(stringHash(shingleArray(i).toLowerCase().mkString(""), seed = getSeed) % Int.MaxValue)
  }
  resultArray.toSeq
}

  override protected def outputDataType: DataType = new ArrayType(LongType, false)

}