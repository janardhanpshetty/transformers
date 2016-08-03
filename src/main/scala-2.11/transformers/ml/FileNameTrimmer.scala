package transformers.ml

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StringType, DataType}

/**
 * Created by jshetty on 8/2/16.
 */

class FileNameTrimmer(override val uid: String) extends UnaryTransformer[String, String, FileNameTrimmer]{

    def this() = this(Identifiable.randomUID("fileNameTrimmer"))

    override protected def createTransformFunc: (String) => String = {
      _.split("/").last
    }

    override protected def outputDataType: DataType = StringType

}
