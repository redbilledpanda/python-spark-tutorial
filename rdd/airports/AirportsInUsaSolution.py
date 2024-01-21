import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[2])

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

    # create an RDD of strings from a text file
    airports = sc.textFile("in/airports.text")

    # filter each element of the RDD (of course in parallel) and then filter it so
    # only lines that have "United States" in the 4th place get through
    # the filter method returns another RDD
    airportsInUSA = airports.filter(lambda line : Utils.COMMA_DELIMITER.split(line)[3] == "\"United States\"")

    # since this is a brand new RDD, we can now use the map method to create a new RDD
    # this RDD will only contain the airport and city names in each line
    airportsNameAndCityNames = airportsInUSA.map(splitComma)
    airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text")
