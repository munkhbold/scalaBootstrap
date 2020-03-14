import scala.util.Try

// step 1
val data = sc.textFile("mtcars.csv").cache()

def isNumeric(input: String): Boolean = input.forall(_.isDigit)
def isDouble(input: String): Boolean = Try(input.toDouble).isSuccess
def getCyl(in:String) : String = { in.split(",")(2)}
def getMpg(in:String) : String = { in.split(",")(1)}

def summationMean(p: Double, v: Double, qty:Int): (Double, Int) = (p + v, qty + 1)
def summationVariance(p: Double, v: Double, qty:Int) : (Double, Int) = (p + (v * v), qty + 1)
def calcStanDev(variance: Double, mean: Double): Double = scala.math.sqrt(variance - scala.math.pow(mean, 2)).toDouble

// step 2
val cleanData = data.map(x=>(getCyl(x), getMpg(x))).filter(x=>isNumeric(x._1) && isDouble(x._2)).map(x=>(x._1, x._2.toDouble))
val dsList = cleanData.sortByKey().collect().toList

// step 3
def computeAll(dataList:List[(String, Double)]) {
    val meanData = collection.mutable.Map[String, (Double, Int)]()
    val varianceData = collection.mutable.Map[String, (Double, Int)]()
    for((k, v)<- dataList){
        if(meanData.get(k) == None){
            meanData(k) = (v, 1)
            varianceData(k) = (v * v, 1)
        } else {
            var qty = meanData(k)._2
            meanData(k) = summationMean(meanData(k)._1, v, qty)
            varianceData(k) = summationVariance(varianceData(k)._1, v, qty)
        }
    }

    for( (k,v) <- meanData){
        var row = varianceData(k)
        var mean = v._1/v._2
        
        var variance = calcStanDev(row._1/row._2, mean)
        println(k + " " + mean + " " + scala.math.sqrt(variance.toDouble))
    }
}

// step 4
val dataList = cleanData.sample(false, 1).collect().toList

def bootstrapping(dataList:List[(String, Double)], n: Int){
    val meanDic = collection.mutable.Map[String, (Double, Int)]()
    val varianceDic = collection.mutable.Map[String, (Double, Int)]()
    val dataSet = sc.parallelize(dataList)
    // step 5
    for( i <- 1 to n){
        var s = dataSet.sample(false, 1).collect().toList
        for( i <- s){
            var k = i._1
            var v = i._2
            if(meanDic.get(k) == None){
                meanDic(k) = (v, 1)
                varianceDic(k) = (v * v, 1)
            } else{
                var qty = meanDic(k)._2
                meanDic(k) = summationMean(meanDic(k)._1, v, qty)
                varianceDic(k) = summationVariance(varianceDic(k)._1, v, qty)
            }
        }
    }

    // step 6
    for((k,v) <- meanDic){
        var row = varianceDic(k)
        var mean = v._1/v._2
        var variance = calcStanDev(row._1/row._2, mean)
        println(k + " " + mean + " " + variance)
    }
}

computeAll(dsList)
bootstrapping(dataList, 100)