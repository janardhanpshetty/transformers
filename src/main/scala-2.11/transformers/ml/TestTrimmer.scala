package transformers.ml

import org.apache.spark.sql.SparkSession

/**
 * Created by jshetty on 8/2/16.
 */
object TestTrimmer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[8]").appName("Transformer").getOrCreate()
    val wordDataFrame = spark.createDataFrame(Seq((0, "hdfs://localhost:9000/user/jshetty/documentsimilarity/dataset/testdocs/Doc1.txt"), (1, "hdfs://localhost:9000/user/jshetty/documentsimilarity/dataset/testdocs/Doc2.txt"), (2, "hdfs://localhost:9000/user/jshetty/documentsimilarity/dataset/testdocs/Doc3a.txt"))).toDF("label", "words")

    val trimmer = new FileNameTrimmer().setInputCol("words").setOutputCol("filename")
    val capitalizerDF = trimmer.transform(wordDataFrame)
    capitalizerDF.show()

  }

}
