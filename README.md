# Locality Sensitive Hashing for Spark #

Locality-sensitive hashing (LSH) is an approximate nearest neighbor search and clustering method. Locality-Sensitive functions take two items and decide about whether or not they should be a candidate pair. LSH hashes input items in a way that similar items map to the same "buckets" with a high probability than dissimilar items. The items mapped to the same buckets are considered as candidate pair. 

There are different LSH schemes for different distance measures. This implementation is based on Charikar's LSH schema for cosine distance described in [Similarity Estimation Techniques from Rounding Algorithms](http://www.cs.princeton.edu/courses/archive/spr04/cos598B/bib/CharikarEstim.pdf) paper. This scheme uses random hyperplane based hash functions for collection of vectors to produce hash values. This implementation combines banding technique with TODO described in [Mining of Massive Datasets](http://mmds.org) book to reduce the false positives and false negatives.

for [Apache Spark](https://spark.apache.org) 

### Build ###

This is an SBT project. You will need SBT (http://www.scala-sbt.org) to build this project. 
```
#!shell

sbt clean compile
```
