# Locality Sensitive Hashing for Spark #

Locality-sensitive hashing (LSH) is an approximate nearest neighbor search and clustering method. Locality-Sensitive functions take two items and decide about whether or not they should be a candidate pair. LSH hashes input items in a way that similar items map to the same "buckets" with a high probability than dissimilar items. The items mapped to the same buckets are considered as candidate pair.

There are different LSH schemes for different distance measures. This implementation is for [Apache Spark](https://spark.apache.org) and based on LSH scheme for cosine distance which operates on collection of vectors. Charikar described this scheme in [Similarity Estimation Techniques from Rounding Algorithms](http://www.cs.princeton.edu/courses/archive/spr04/cos598B/bib/CharikarEstim.pdf) paper. This scheme uses random hyperplane based hash functions for vectors to produce hash values. It also combines banding technique described in [Mining of Massive Datasets](http://mmds.org) book to reduce the false positives and false negatives.

### What is this repository for? ###

* Quick summary
* Version
* [Learn Markdown](https://bitbucket.org/tutorials/markdowndemo)