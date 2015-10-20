# Locality Sensitive Hashing for Apache Spark #

Locality-sensitive hashing (LSH) is an approximate nearest neighbor search and 
clustering method for high dimensional data points (http://www.mit.edu/~andoni/LSH/). 
Locality-Sensitive functions 
take two data points and decide about whether or not they should be a candidate 
pair. LSH hashes input data points multiple times in a way that similar data 
points map to the same "buckets" with a high probability than dissimilar data 
points. The data points map to the same buckets are considered as candidate pair. 

There are different LSH schemes for different distance measures. This implementation 
is based on Charikar's LSH schema for cosine distance described in 
[Similarity Estimation Techniques from Rounding Algorithms]
(http://www.cs.princeton.edu/courses/archive/spr04/cos598B/bib/CharikarEstim.pdf) 
paper. This scheme uses random hyperplane based hash functions for collection of 
vectors to produce hash values. The model build (preprocessing) and query answering 
algorithms implemented as described in Figures 1 and 2 of http://www.vldb.org/conf/1999/P49.pdf.

## Build ##

This is an SBT project.
```
sbt clean compile
```

## Usage ##

"Main.scala" provided in this package contains sample code for usage of LSH package. 
In the following sections we will demonstrate usage of LSH to group similar users 
that rate on items. We will use famous movie-lens data set for demonstration. The 
data set contains user/item ratings in (user::item::rating::time) format. The 
zipped version of data set is provided in "data" directory of this project. 

We would like to group similar users by using LSH method. As preprocessing step 
we read the data set and create RDD of Tuple3 version of data in (user, item, rating)
format:

```scala
//read data file in as a RDD, partition RDD across <partitions> cores
val data = sc.textFile(dataFile, numPartitions)

//parse data and create (user, item, rating) tuple
val ratingsRDD = data
    .map(line => line.split("::"))
    .map(elems => (elems(0).toInt, elems(1).toInt, elems(2).toDouble))
```


We need to represent each user as a vector of ratings to be able to calculate 
cosine similarity of users. In order to convert rating of a user to SparseVector,
we need to determine the possible largest vector index in data set. We set it to
maximum index of items since item numbers will be indices of SparseVector. Maximum 
index value also will be used for generating random vectors in hashers. 

```scala
//list of distinct items
val items = ratingsRDD.map(x => x._2).distinct()
val maxIndex = items.max + 1
```

Now we are ready to convert users data to RDD of Tuple2 as (user_id, SparseVector). 
SparseVector of a user is created by using a list of (item, rating) pairs as (index, 
value) pairs.


```scala
//user ratings grouped by user_id
val userItemRatings = ratingsRDD.map(x => (x._1, (x._2, x._3))).groupByKey().cache()

//convert each user's rating to tuple of (user_id, SparseVector_of_ratings)
val sparseVectorData = userItemRatings
    .map(a=>(a._1.toLong, Vectors.sparse(maxIndex, a._2.toSeq).asInstanceOf[SparseVector]))
```

Finally, we use sparseVectorData to build LSH model as follows: 

```scala
//run locality sensitive hashing model with 6 hashTables and 8 hash functions
val lsh = new LSH(sparseVectorData, maxIndex, numHashFunc = 8, numHashTables = 6)
val model = lsh.run()
```

Number of hash functions (number of rows) for each hashTable and number of hashTables
need to be given to LSH. See implementation details for more information for
selecting number of hashTables and hash functions.

```scala
//print sample hashed vectors in ((hashTableId#, hashValue), user_id) format
model.hashTables.take(10) foreach println
```

Sample 10 entries from the model printed out as follows:

```
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

### Find Similar Users for User ID ###
Find the similar users for user id: 4587 as follows:

```scala
//get the near neighbors of userId: 4587 in the model
val candList = model.getCandidates(4587)
println("Number of Candidates: "+ candList.count())
println("Candidate List: " + candList.collect().toList)
```

172 neighbors found for user 4587:

```
Number of Candidates: 172
Candidate List: List(1708, 5297, 1973, 4691, 2864, 903, 30, 501, 2433, 3317, 2268, 4759, 1593, 2617, 3794, 2958, 5918, 3743, 1527, 5030, 1271, 4713, 4095, 2615, 1948, 597, 818, 1084, 5592, 3334, 2342, 3740, 2647, 3476, 2115, 2676, 1385, 2606, 1809, 584, 2341, 5063, 320, 1162, 4899, 5343, 5998, 1423, 1374, 2121, 1846, 3985, 529, 5654, 810, 1028, 5727, 1549, 3126, 2376, 3258, 5573, 5291, 1752, 4727, 187, 1159, 2114, 1028, 4747, 4852, 2390, 3404, 900, 5016, 3576, 5855, 1959, 2964, 2171, 5940, 2521, 171, 5375, 2125, 3357, 2217, 1227, 5949, 2722, 4943, 1575, 1319, 1529, 618, 370, 1280, 5164, 5340, 1166, 4332, 1845, 4158, 5724, 1938, 4953, 2128, 492, 595, 3852, 2915, 4789, 159, 124, 989, 4702, 4259, 2733, 2623, 5431, 1398, 4172, 629, 86, 2726, 5690, 563, 5977, 3538, 2476, 1855, 2904, 3168, 769, 4429, 1470, 1829, 1461, 5335, 5125, 922, 5772, 5109, 643, 131, 4421, 5259, 1960, 738, 383, 5906, 1989, 1902, 469, 500, 15, 939, 1292, 53, 5437, 3721, 3143, 5393, 1789, 1465, 2519, 3001, 4016, 5967, 3203, 3295, 5208)
```

### Find Similar Users for Vectors ###

We will find the similar users to a user by using user's rating data on movies. We first 
convert this data to a SparseVector as follows:
```scala
val movies = List(1,6,17,29,32,36,76,137,154,161,172,173,185,223,232,235,260,272,296,300,314,316,318,327,337,338,348)
val ratings = List(5.0,4.0,4.0,5.0,5.0,4.0,5.0,3.0,4.0,4.0,4.0,4.0,4.0,5.0,5.0,4.0,5.0,5.0,4.0,4.0,4.0,5.0,5.0,5.0,4.0,4.0,4.0)
val sampleVector = Vectors.sparse(maxIndex, movies zip ratings).asInstanceOf[SparseVector]
```
Then query LSH model for candidate user list for sampleVector:
```scala
val candidateList = model.getCandidates(sampleVector)
println(candidateList.collect().toList)
```

Following user list is returned as candidate list:
```
List(3925, 4607, 3292, 2919, 240, 4182, 5244, 1452, 4526, 3831, 305, 4341, 2939, 2731, 627, 5685, 1656, 3597, 3268, 2908, 1675, 5124, 4588, 5112, 4620, 890, 3655, 5642, 4737, 372, 5916, 3806, 6037, 5384, 1888, 4059, 996, 660, 889, 5020, 2871, 2107, 5080, 1638, 588, 4486, 2945, 335, 2013, 363, 1257, 117, 2848, 417, 1101, 2171, 4526, 147, 411, 3709, 3941, 904, 4442, 1576, 1177, 3844, 5527, 5280, 2998, 287, 3575, 4461, 1548, 5698, 2039, 5283, 5454, 1288, 741, 1496, 11, 3829, 4201, 985, 3862, 2908, 3658, 3594, 5970, 1115, 5690, 5082, 5707, 6030, 555, 4260, 780, 6028, 1353, 5433, 1593, 3933, 5328, 3649, 2700, 3117, 215, 4944, 4266, 3388, 5079, 1483, 1762, 2654)
```

### Find Similarity of Vectors ###

Let *a* and *b* two sparse vectors for two users. We can find similarity of these 
users based on cosine similarity as follows: 
```scala
val similarity = lsh.cosine(a, b)
```

### Hash Values for Vectors ###

We can retrieve hash values for a vector as follows:
```scala
val hashValues = model.hashValue(sampleVector)
println(hashValues)
```

Generated list of hash values for each hashTable in (hashTable#, hashValue) format:

```
List((0,10101100), (5,01110100), (1,01001110), (2,10000000), (3,10101111), (4,00101100))
```

Note that these are the bucket IDs that the vector maps to in each hash table. 
 
 
### Hash Values ###

We can retrieve list of hashValues in hash tables as follows:

```scala
val hashValues = hashTables.map(x => x._1).groupByKey()
```

This returns an RDD [(Int, Iterable [String])]  


### Add New User ###

We add new user with ratings vector as follows:
```scala
val model = model.add(id, v, sc)
```
where id, v, and sc are user id, SparseVector of ratings, and SparkContext respectively.

### Remove an Existing User ###

We delete an existing user from the model as follows:
```scala
val model = model.remove(id, sc)
```
where id is user id and sc is SparkContext.

### Save/Load The Model ###

Trained model can be saved and loaded to/from HDFS as follows:

```scala
//save model
val temp = "target/" + System.currentTimeMillis().toString
model.save(sc, temp)

//load model
val modelLoaded = LSHModel.load(sc, temp)

//print out 10 entries from loaded model
modelLoaded.hashTables.take(10) foreach println
```
Sample 10 entries from loaded model printed out as follows:

```
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
```

## Implementation Details ##

- LSH hashes each vector multiple times (b * r) with hash functions, where *b* is number
of hash tables (bands) and *r* is number of rows (hash functions) in each hash table.

- If we define *t* as similarity threshold for vectors to be considered as a desired
“similar pair.” The threshold *t* is approximately (1/b)<sup>1/r</sup>. Select *b* and *r*
to produce a threshold lower than *t* to avoid false negatives, select *b* and *r* to
produce a higher threshold to increase speed and decrease false positives (See section
3.4.3 of [Mining of Massive Datasets] (http://mmds.org) for details.)

- Hasher function is defined in com.lendap.spark.lsh.Hasher class and uses random hyperplane
based hash functions which operate on vectors.

- Hasher functions use randomly generated vectors whose elements are in [-1, 1]
interval. It is sufficiently random if we randomly select vectors whose components
are +1 and -1 (See section 3.7.3 of [Mining of Massive Datasets] - http://mmds.org.)

- Hashing function calculates dot product of an input vector with a randomly generated 
hash function then produce a hash value (0 or 1) based on the result of dot product. 
Each hasher produce a hash value for the vector. Then all hash values are combined
with *AND-construction* to produce a hash signature (e.g. 11110010) for the input vector.

- Hash signatures for the input vectors are used as bucket ids as described in 
 http://www.vldb.org/conf/1999/P49.pdf. The model build (preprocessing) and 
 query answering algorithms implemented as described in Figures 1 and 2 of this paper.

- Hashed vectors are stored in model.hashTables as *RDD[((Int, String), Long)]* where each entry
is *((hashTable#, hash_value), vector_id)* data.

- The results can be filtered by passing a filter function to the model. 

- Trained model can be saved to HDFS with *model.save* function.
 
- Saved model can be loaded from HDFS with *model.load* function.

