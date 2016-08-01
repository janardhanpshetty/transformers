package transformers.ml

/**
 * Created by jshetty on 7/31/16.
 */

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types._

/**
 * Created by jshetty on 7/30/16.
 * Tranformer to capitalize string
 */

class Capitalizer(override val uid: String) extends UnaryTransformer[String, String, Capitalizer] {

  def this() = this(Identifiable.randomUID("capitalizer"))

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = StringType

  override def createTransformFunc: String => String = {
    _.toLowerCase().capitalize
  }

  override def copy(extra: ParamMap): Capitalizer = defaultCopy(extra)
}


