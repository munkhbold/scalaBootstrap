import scala.util.Try
import scala.collection.mutable.ListBuffer

// step 1
val data = sc.textFile("mtcars.csv")

def isNumeric(input: String): Boolean = input.forall(_.isDigit)
def isDouble(input: String): Boolean = Try(input.toDouble).isSuccess
def getCyl(in:String) : String = { in.split(",")(2)}
def getMpg(in:String) : String = { in.split(",")(1)}

// step 2
val cleanData = data.map(x=>(getCyl(x), getMpg(x))).filter(x=>isNumeric(x._1) && isDouble(x._2)).map(x=>(x._1, x._2.toDouble))
val dsList = cleanData.sortByKey().collect().toList

// step 3
def computeAll(dataList:List[(String, Double)]) {
    val meanData = collection.mutable.Map[String, (Double, Int)]()
    val varianceData = collection.mutable.Map[String, (Double, Int)]()
    for((k, v)<- dataList){
        if(meanData.get(k) == None)
            meanData(k) = (0, 0)

        var qty = meanData(k)._2
        var sum = meanData(k)._1
        meanData(k) = (sum + v, qty + 1)
    }

    for((k, v)<- dataList){
        val row = meanData(k)
        val mean = row._1 / row._2
        val x = v - mean
        if(varianceData.get(k) == None)
            varianceData(k) = (0, 0)

        var qty = varianceData(k)._2
        var sum = varianceData(k)._1
        varianceData(k) = (sum + (x * x), qty + 1)
    }

    for( (k,v) <- meanData){
        var mean = v._1/v._2
        var variance = varianceData(k)
        println(k + " " + mean + " " + variance._1/ variance._2)
    }
}

// step 4
val dataList = cleanData.sample(false, 0.25).collect().toList

def bootstrapping(dataList:List[(String, Double)], n: Int){
    val meanData = collection.mutable.Map[String, (Double, Int)]()
    val varianceData = collection.mutable.Map[String, (Double, Int)]()
    val dataSet = sc.parallelize(dataList)
    var arrList = new ListBuffer[List[(String, Double)]]()
    // step 5
    for( i <- 1 to n){
        var s = dataSet.sample(true, 1).collect().toList
        for( i <- s){
            var k = i._1
            var v = i._2
            if(meanData.get(k) == None)
                meanData(k) = (0, 0)

            var sum = meanData(k)._1
            var qty = meanData(k)._2
            meanData(k) = (sum + v, qty + 1)
        }
        arrList += s
    }

    for(arr <- arrList){
        for((k, v)<- arr){
            val row = meanData(k)
            val mean = row._1 / row._2
            val x = v - mean
            if(varianceData.get(k) == None)
                varianceData(k) = (0, 0)

            var sum = varianceData(k)._1
            var qty = varianceData(k)._2
            varianceData(k) = (sum + (x * x), qty + 1)
        }
    }

    // step 6
    for( (k,v) <- meanData){
        var row = varianceData(k)
        var mean = v._1/v._2
        println(k + " " + mean + " " + row._1/row._2)
    }
}

computeAll(dsList)
bootstrapping(dataList, 1000)