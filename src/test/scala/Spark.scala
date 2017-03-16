import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.slf4j.LoggerFactory

class Spark extends FlatSpec with Matchers with BeforeAndAfter {

  val logger = LoggerFactory.getLogger(classOf[Spark])

  "Test" should "" in {
    val master = "local[*]"
    val conf = new SparkConf().setAppName(classOf[Spark].getName).setMaster(master)
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq("my name is Titi", "my name is Toto"))
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y).collect()

    rdd.foreach(println)

    assert(rdd.length == 5)
  }
}
