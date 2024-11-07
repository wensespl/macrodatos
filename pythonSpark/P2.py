val bdsales = sc.textFile("SalesJna2009.csv")
bdsales.collect()
val mp = bdsales.mapPartitionsWithIndex{ (idx,iter) => if(idx==0) iter.drop(1) else iter }
mp.collect()
mp.take(2)

val venpa = mp.map(s => s.split(",")(7)).map(word => (word,1)).reduceByKey(_+_)
vanpa.collect()
venpa.take(2)
venpa.saveAsTextFile("01output_venpa")