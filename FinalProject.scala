package mtcars

import org.apache.spark.{SparkConf, SparkContext}

case class Pair(cyl: String, mpg: Double)

object FinalProject {
  def main(args: Array[String]): Unit = {
    val conf = new  SparkConf().setMaster("local[2]").setAppName("AnyName").set("spark.driver.host", "localhost");
    val sc = new SparkContext(conf);

    sc.setLogLevel("ERROR")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val n = 10
    var sampleSize = 10
    var mapMean = collection.mutable.Map[String, Double]()
    var mapVar = collection.mutable.Map[String, Double]()
    var mapCounter = collection.mutable.Map[String, Int]()
    mapCounter("4") = n
    mapCounter("6") = n
    mapCounter("8") = n

    val csv = sc.textFile("C:/data/mtcars.csv")
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val data = headerAndRows.filter(_ (0) != header(0))
    val pairs = data.map(p => Pair(p(2), p(1).toDouble)).toDF()

    println("=========== DESIRED OUTPUT ===============")
    pairs.createOrReplaceTempView("pairs")
    val desiredOutput = sqlContext.sql("SELECT cyl, " +
                                                  "avg(mpg) as mean, " +
                                                  "stddev(mpg) as variance " +
                                                  "FROM pairs group by cyl")
    desiredOutput.show()

    println("============ SAMPLE PAIRS ===============")
    val samplePairs = sc.parallelize(pairs.rdd.takeSample(false, sampleSize)).map(p => Pair(p(0).toString, p(1).asInstanceOf[Double])).toDF()
    samplePairs.show()
    for (i <- 1 to n) {
      val bootstrapPairs = sc.parallelize(samplePairs.rdd.takeSample(true, sampleSize)).map(p => Pair(p(0).toString, p(1).asInstanceOf[Double])).toDF()
      bootstrapPairs.createOrReplaceTempView("boot")
      val result = sqlContext.sql("SELECT cyl, " +
                                            "avg(mpg) as mean, " +
                                            "stddev(mpg) as variance " +
                                            "FROM boot group by cyl")
      result.collect().toList.foreach(row => {
        val key = row.get(0).asInstanceOf[String]
        val mean = row.get(1).asInstanceOf[Double]
        val Var = row.get(2).asInstanceOf[Double]
        if (!java.lang.Double.isNaN(Var) && !java.lang.Double.isNaN(mean)) {
          if (!mapMean.contains(key))
            mapMean(key) = mean
          else
            mapMean(key) = mapMean(key) + mean
          if (!mapVar.contains(key))
            mapVar(key) = Var
          else
            mapVar(key) = mapVar(key) + Var
        }
        else
          mapCounter(key) = mapCounter(key) - 1
      })
    }
    println("=========== ACTUAL OUTPUT ================")
    for ((x, y) <- mapMean) {
      mapMean(x) = mapMean(x) / mapCounter(x);
      mapVar(x) = mapVar(x) / mapCounter(x);
      println(x + " | " + mapMean(x) + " | " + mapVar(x))
    }
  }
}
