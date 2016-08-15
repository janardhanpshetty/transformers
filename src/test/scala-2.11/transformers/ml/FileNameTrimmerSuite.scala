package transformers.ml

import org.scalatest._

/**
 * Created by jshetty on 8/14/16.
 */
class FileNameTrimmerSuite extends FunSuite with SparkFunContext {
   test("FileName Trimmer test"){
  val wordDataFrame = spark.createDataFrame(Seq((0, "hdfs://localhost:9000/user/jshetty/documentsimilarity/dataset/testdocs/Doc1.txt"),
    (1, "hdfs://localhost:9000/user/jshetty/documentsimilarity/dataset/testdocs/Doc2.txt"),
    (2, "hdfs://localhost:9000/user/jshetty/documentsimilarity/dataset/testdocs/Doc3a.txt"))).toDF("label", "words")

  val trimmer = new FileNameTrimmer().setInputCol("words").setOutputCol("filename").transform(wordDataFrame)

  val expected = spark.createDataFrame(Seq((0, "hdfs://localhost:9000/user/jshetty/documentsimilarity/dataset/testdocs/Doc1.txt", "Doc1.txt"),
    (1, "hdfs://localhost:9000/user/jshetty/documentsimilarity/dataset/testdocs/Doc2.txt", "Doc2.txt"),
    (2, "hdfs://localhost:9000/user/jshetty/documentsimilarity/dataset/testdocs/Doc3a.txt", "Doc3a.txt"))).toDF("label", "words", "filename")

  assert(trimmer.collect().deep == expected.collect().deep)
}
}
