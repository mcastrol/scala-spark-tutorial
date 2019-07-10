import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")

///*CONNECTION TO CLUSTER*/
val sc = new SparkContext(conf)

val inputInteger = List(1,5, 6,7,9)
/*CREATE AN RDD */
val integerRDD = sc.parallelize(inputInteger)

//create rdd from external file
val lines = sc.textFile("/home/marcela/gitprojects/scala-spark-tutorial/in/word_count.text")

//apply a transformation filter
val cleanedLines = lines.filter(line => !line.isEmpty)

//apply a transformation map
val lenght = cleanedLines.map(line => line.length)


//rdd AirportsInUsaSolution example of filter and map using csv
//rdd AirportsInLatitudeSolution example of filter and map using csv
//rdd WordCound example of flatmap

// nasaApacheWebLogs UnionLogSolution: example of Union & filter header and select sample rdd
// nasaApacheWebLogs: SameHostProblem example of intersection &  filter header and select sample rdd

var longitud = lenght.collect()
for (len  <-longitud) println(len)

//in rdd.count CountExample: use of count and CoundByValue
//val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
//val wordRdd = sc.parallelize(inputWords)
//println("Count: " + wordRdd.count())
//
//val wordCountByValue = wordRdd.countByValue()
//println("CountByValue:")

//in rdd.take TakeExample
//for ((word, count) <- wordCountByValue) println(word + " : " + count)
//
//val words = wordRdd.take(3)
//for (word <- words) println(word)

//in rdd reduce ReduceExample sumOfNumbersProblem: take 100 first and space separator reading

//rdd persist PersistExample: example of Persist

//pairRdd create: creation of RDD key-value
//pairRdd filter: filter of RDD key-value
//pairRdd mapvalue: filter of RDD key-value
//pairRdd reducebykey: WordCount (mas eficiente que countValue)
//pairRdd reducebykey: houseprice: AverageHousePriceSolution reducebykey+mapvalues
//pairRdd groupbykey: AirportsByCountrySolution
// reducebykey=groupbykey but is better for larger files.
//pairrdd sort sortbyKey examples with print examples. we can use sortBy as well

// particionado do hash + persist to be sure
// benefits from partiotions: join / leftOuterJoin rightOuterJoin groupByKey reduceByKey lookup
// map puede estar afectado  puede afectar ek hash y modifical la clave  - Usar mapValues en vez de map

//rdd join left right and full

//sparksql stackoverflow  sparksql api example
//sparksql UkMakerSpaces leftjoin - concat startsWith

//sparksql using TypedDataset
