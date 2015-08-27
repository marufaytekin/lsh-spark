# Locality Sensitive Hashing for Apache Spark #

Locality-sensitive hashing (LSH) is an approximate nearest neighbor search and clustering method (http://www.mit.edu/~andoni/LSH/). Locality-Sensitive functions take two items and decide about whether or not they should be a candidate pair. LSH hashes input items in a way that similar items map to the same "buckets" with a high probability than dissimilar items. The items mapped to the same buckets are considered as candidate pair. 

There are different LSH schemes for different distance measures. This implementation is based on Charikar's LSH schema for cosine distance described in [Similarity Estimation Techniques from Rounding Algorithms](http://www.cs.princeton.edu/courses/archive/spr04/cos598B/bib/CharikarEstim.pdf) paper. This scheme uses random hyperplane based hash functions for collection of vectors to produce hash values and banding technique (see [Mining of Massive Datasets](http://mmds.org) book) to reduce the false positives and false negatives.

## Build ##

This is an SBT project.
```
#!shell

sbt clean compile
```

## Usage ##

Let's use LSH to group similar users that rated on items. We will use famous movie-lens data set which contains user/item ratings in (user::item::rating::time) format for demonstration. The zipped version of data set is provided in "data" directory of this project. 

We would like to group users with similar ratings. As preprocessing step we read the data set and create RDD of Tuple3 version of data set as follows:

```
#!scala
//read data file in as a RDD, partition RDD across <partitions> cores
val data = sc.textFile(dataFile, numPartitions)

//parse data and create (user, item, rating) tuple
val ratingsRDD = data
    .map(line => line.split("::"))
    .map(elems => (elems(0).toInt, elems(1).toInt, elems(2).toDouble))
```

We need to represent each user as a vector of ratings to be able to calculate similarity of users. 

We determine the possible largest vector index in data set as the maximum index of items. This is used for generating random vectors in hashers. 

```
#!scala
//list of distinct items
val items = ratingsRDD.map(x => x._2).distinct()
val maxIndex = items.max + 1
```

We convert users data to RDD of Tuple2 as (user_id, SparseVector). SparseVector of a user is created by using a list of (item, rating) pairs as (index, value) pairs.

```
#!scala
//user ratings grouped by user_id
val userItemRatings = ratingsRDD.map(x => (x._1, (x._2, x._3))).groupByKey().cache()

//convert each user's rating to tuple of (user_id, SparseVector_of_ratings)
val sparseVectorData = userItemRatings
    .map(a=>(a._1.toLong, Vectors.sparse(maxIndex, a._2.toSeq).asInstanceOf[SparseVector]))
```

Now we can use sparseVectorData to build LSH model. 

```
#!scala
//run locality sensitive hashing model with 6 bands and 8 hash functions
val lsh = new LSH(sparseVectorData, maxIndex, numHashFunc = 8, numBands = 6)
val model = lsh.run()

//print sample hashed vectors in ((bandId#, hashValue), user_id) format
model.bands.take(10) foreach println
```
Output is as follows:

```
#!shell
((1,10100000),4289)
((5,01001100),649)
((3,10011011),5849)
((0,11000110),5221)
((1,01010100),3688)
((1,00001110),354)
((0,11000110),5118)
((3,00001011),3698)
((3,11010011),2941)
((2,11010010),4488)
```

### Find Similar Users with ID ###
Find the similar users for user id: 4587 as follows:

```
#!scala
//get the near neighbors of userId: 4587 in the model
val candList = model.getCandidates(4587)
println("Number of Candidate Neighbors: "+ candList.count())
println("Candidate List: " + candList.collect().toList)
```

172 neighbors found for user: 4587:

```
#!shell
Number of Candidate Neighbors: 172
Candidate List: List(1708, 5297, 1973, 4691, 2864, 903, 30, 501, 2433, 3317, 2268, 4759, 1593, 2617, 3794, 2958, 5918, 3743, 1527, 5030, 1271, 4713, 4095, 2615, 1948, 597, 818, 1084, 5592, 3334, 2342, 3740, 2647, 3476, 2115, 2676, 1385, 2606, 1809, 584, 2341, 5063, 320, 1162, 4899, 5343, 5998, 1423, 1374, 2121, 1846, 3985, 529, 5654, 810, 1028, 5727, 1549, 3126, 2376, 3258, 5573, 5291, 1752, 4727, 187, 1159, 2114, 1028, 4747, 4852, 2390, 3404, 900, 5016, 3576, 5855, 1959, 2964, 2171, 5940, 2521, 171, 5375, 2125, 3357, 2217, 1227, 5949, 2722, 4943, 1575, 1319, 1529, 618, 370, 1280, 5164, 5340, 1166, 4332, 1845, 4158, 5724, 1938, 4953, 2128, 492, 595, 3852, 2915, 4789, 159, 124, 989, 4702, 4259, 2733, 2623, 5431, 1398, 4172, 629, 86, 2726, 5690, 563, 5977, 3538, 2476, 1855, 2904, 3168, 769, 4429, 1470, 1829, 1461, 5335, 5125, 922, 5772, 5109, 643, 131, 4421, 5259, 1960, 738, 383, 5906, 1989, 1902, 469, 500, 15, 939, 1292, 53, 5437, 3721, 3143, 5393, 1789, 1465, 2519, 3001, 4016, 5967, 3203, 3295, 5208)
```

### Find Similar Users for Vector ###

Let's find the similar users to a user whose rating data on movies as follows:  
  
```
#!shell
movies = List(1,6,17,29,32,36,76,137,154,161,172,173,185,223,232,235,260,272,296,300,314,316,318,327,337,338,348)
ratings = List(5.0,4.0,4.0,5.0,5.0,4.0,5.0,3.0,4.0,4.0,4.0,4.0,4.0,5.0,5.0,4.0,5.0,5.0,4.0,4.0,4.0,5.0,5.0,5.0,4.0,4.0,4.0)
```
We convert this data to a SparseVector:
```
#!scala
val movies = List(1,6,17,29,32,36,76,137,154,161,172,173,185,223,232,235,260,272,296,300,314,316,318,327,337,338,348)
val ratings = List(5.0,4.0,4.0,5.0,5.0,4.0,5.0,3.0,4.0,4.0,4.0,4.0,4.0,5.0,5.0,4.0,5.0,5.0,4.0,4.0,4.0,5.0,5.0,5.0,4.0,4.0,4.0)
val sampleVector = Vectors.sparse(maxIndex, movies zip ratings).asInstanceOf[SparseVector]
```
Query LSH model for candidate user list sampleVector:
```
#!scala
val candidateList = model.getCandidates(sampleVector)
println(candidateList.collect().toList)
```

Outputs candidate user list:
```
#!shell
List(3925, 4607, 3292, 2919, 240, 4182, 5244, 1452, 4526, 3831, 305, 4341, 2939, 2731, 627, 5685, 1656, 3597, 3268, 2908, 1675, 5124, 4588, 5112, 4620, 890, 3655, 5642, 4737, 372, 5916, 3806, 6037, 5384, 1888, 4059, 996, 660, 889, 5020, 2871, 2107, 5080, 1638, 588, 4486, 2945, 335, 2013, 363, 1257, 117, 2848, 417, 1101, 2171, 4526, 147, 411, 3709, 3941, 904, 4442, 1576, 1177, 3844, 5527, 5280, 2998, 287, 3575, 4461, 1548, 5698, 2039, 5283, 5454, 1288, 741, 1496, 11, 3829, 4201, 985, 3862, 2908, 3658, 3594, 5970, 1115, 5690, 5082, 5707, 6030, 555, 4260, 780, 6028, 1353, 5433, 1593, 3933, 5328, 3649, 2700, 3117, 215, 4944, 4266, 3388, 5079, 1483, 1762, 2654)
```

### Hash Values for Vectors ###

Retrieve hash values for a vector:

#!scala
val hashValues = model.hashValue(sampleVector)
println(hashValues)
```

Generates list of hash values for each band in (band#, hashValue) format:

```
#!shell
List((0,10101100), (5,01110100), (1,01001110), (2,10000000), (3,10101111), (4,00101100))
```

### Add New User ###

We add new user with ratings vector as follows:
```
#!scala
val model = model.add(id, v, sc)
```
where id is user id, v is the SparseVector of ratings for the user, and sc is SparkContext.

### Remove an Existing User ###

We delete an existing user from the model as follows:
```
#!scala
val model = model.remove(id, sc)
```
where id is user id and sc is SparkContext.

### Save/Load The Model ###

Trained model can be saved and loaded to/from HDFS as follows:

```
#!scala
//save model
val temp = "target/" + System.currentTimeMillis().toString
model.save(sc, temp)

//load model
val modelLoaded = LSHModel.load(sc, temp)

//print out 10 entries from loaded model
modelLoaded.bands.take(15) foreach println
```
Output is as follows:

```
#!shell
((1,11101110),4289)
((5,11100001),649)
((3,11001111),5849)
((0,10100101),5221)
((1,01110001),3688)
((1,11110010),354)
((0,10010100),5118)
((3,10011010),3698)
((3,10100010),2941)
((2,11010101),4488)
((3,10111011),2083)
((5,11100011),2895)
((1,11101001),213)
((3,00011110),1296)
((0,10100100),4387)
```

