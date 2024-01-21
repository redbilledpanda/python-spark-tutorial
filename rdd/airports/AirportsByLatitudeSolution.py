import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    # configuration object; sets app name to "airports" and 
    # setMaster set's the master of the cluster. This is usually an
    # IP address or FQDN. In this case we are letting the engine
    # decide what local core to use and also telling it to use all
    # logical cores
    conf = SparkConf().setAppName("airports").setMaster("local[*]")

    # handle to spark cluster, using the above conf obect; this can be used
    # to do all manner of spark functionality like creatng RDDs, accumulators and
    # broadcast variables to the cluster
    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")
    
    # return an RDD of strings
    airports = sc.textFile("in/airports.text")

    airportsInUSA = airports.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)
    
    airportsNameAndCityNames = airportsInUSA.map(splitComma)

    airportsNameAndCityNames.saveAsTextFile("out/airports_by_latitude.text")