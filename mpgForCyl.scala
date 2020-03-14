package mtcars
import org.apache.spark.{SparkConf, SparkContext}

case class Pair(cyl: String, mpg: Double)

object mpgForCyl {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("Spark and SparkSql")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val n = 1000
    var counter = n
    var sampleSize = 10
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val csv = sc.textFile("C:/data/mtcars.csv")
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val data = headerAndRows.filter(_ (0) != header(0))
    val pairs = data
      .map(p => Pair(p(2), p(1).toDouble))
      .toDF()
    pairs.createOrReplaceTempView("pairs")
    val desiredOutput = sqlContext.sql("SELECT cyl, avg(mpg) as mean, stddev(mpg) as stddev FROM pairs group by cyl")
    println("=========== DESIRED OUTPUT ===============")
    desiredOutput.show()

    val samplePairs = sc.parallelize(pairs.rdd.takeSample(false, sampleSize)).map(p=>Pair(p(0).toString, p(1).asInstanceOf[Double])).toDF()
    println("========= SAMPLE PAIRS ==============")
    samplePairs.show()
    var mapMean = collection.mutable.Map[String, Double]()
    var mapStd = collection.mutable.Map[String, Double]()
    for (i <- 1 to n) {
      val bootstrapPairs = sc.parallelize(samplePairs.rdd.takeSample(true, sampleSize)).map(p=>Pair(p(0).toString, p(1).asInstanceOf[Double])).toDF()
      println(i+ "======== BOOTSTRAPPED PAIRS ========")
      bootstrapPairs.show()

      bootstrapPairs.createOrReplaceTempView("boot")
      val result = sqlContext.sql("SELECT cyl, avg(mpg) as mean, stddev(mpg) as stddev FROM boot group by cyl")
      result.collect().toList.foreach(row => {
        val key = row.get(0).asInstanceOf[String]
        val mean = row.get(1).asInstanceOf[Double]
        val std = row.get(2).asInstanceOf[Double]
        if (!java.lang.Double.isNaN(std) && !java.lang.Double.isNaN(mean)) {
          if (!mapMean.contains(key))
            mapMean(key) = mean
          else
            mapMean(key) = mapMean(key) + mean
          if (!mapStd.contains(key))
            mapStd(key) = std
          else
            mapStd(key) = mapStd(key) + std
        }
        else
          counter = counter - 1
      })
    }
    println("=========== ACTUAL OUTPUT ================")
    for ((x, y) <- mapMean) {
      mapMean(x) = mapMean(x) / counter;
      mapStd(x) = mapStd(x) / counter;
      println(x + " | " + mapMean(x) +" | "+ mapStd(x))
    }
    println("Number of valid samples: "+counter)
  }
}
