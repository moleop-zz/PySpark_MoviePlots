from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import functions as F

def quiet_logs(sc):
	logger = sc._jvm.org.apache.log4j
	logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
	logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
# run on cluster
confCluster = SparkConf()
confCluster.setAppName("Spark Test Cluster")
# run on local machine
confLocal = SparkConf().setMaster("local").setAppName("Spark Test Local")
sc = SparkContext(conf = confCluster)

# Verarbeiten von Textdatei mit Jahren fuer Filteroperation
yearinput = sc.textFile("movieplots/years.txt")
years = yearinput.filter(lambda line: '-' not in line and len(line) >= 1)
yearranges = yearinput.filter(lambda line: '-' in line).flatMap(lambda line: range(int(line.split("-")[0]),int(line.split("-")[1])+1))
newyears = yearranges.map(lambda x: unicode(x))
relevantyears = years.union(newyears)

origininput = sc.textFile("movieplots/origins.txt")
origins = origininput.flatMap(lambda line: line.lower().split(" "))

genreinput = sc.textFile("movieplots/genres.txt")
genres = genreinput.flatMap(lambda line: line.lower().split(" "))

whitelistinput = sc.textFile("movieplots/whitelist.txt")
whitelist = whitelistinput.flatMap(lambda line: line.lower().split(" "))

#Einlesen der Daten (Movieplots)
csvRDD = sc.textFile("movieplots/movieplots_fixed.csv")
movies = csvRDD.map(lambda line: line.lower().split(","))

#Filtern der relevanten Jahre
yearlist = relevantyears.collect()
movies = movies.filter(lambda movie: movie[0] in yearlist or len(yearlist) == 0)
#Filtern relevanter Laender
originlist = origins.collect()
movies = movies.filter(lambda movie: movie[2] in originlist or len(originlist) == 0)
#Filtern relevanter Genres
genrelist = genres.collect()
movies = movies.filter(lambda movie: movie[5] in genrelist or len(genrelist) == 0)

plots = movies.map(lambda x: x[7].split(" "))
plotsets = plots.map(lambda x: set(x))
unique_plots = plotsets.flatMap(lambda x: list(x))

whitelistitems = whitelist.collect()
unique_plots = unique_plots.filter(lambda word: word in whitelistitems or len(whitelistitems) == 0)

wcount = unique_plots.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
#Filtern unwichtiger Woerter
filter_words = sc.textFile("movieplots/filterlist.txt")
filter_list = filter_words.flatMap(lambda line: line.split(" ")).map(lambda x: (x,1))
relevant_words = wcount.subtractByKey(filter_list)
relevant_words = relevant_words.takeOrdered(150, lambda (word,count): -count)

output = str(relevant_words)
#items = relevant_words.takeOrdered(50, lambda (word,count): -count)
#with open('testoutput.csv', 'w') as outfile:
#    spamwriter = csv.writer(outfile, delimiter=',', quotechar='"')
#	for item in items:
#		spamwriter.writerow([str(item[0])]+[str(item[1])])


#items.collect().foreach(println(str(item[0]) + '\t' + str(item[1])).start()
#items.map(lambda row: str(row[0]) + "\t" + str(row[1])) \
   #.saveAsTextFile("movie_output")
#output = [str(item[0])+"\t"+str(item[1])+"\n" for item in items]
#print(output)
sc.parallelize([output]).saveAsTextFile("movie_output")
