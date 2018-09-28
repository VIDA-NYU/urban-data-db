Urban Data Integration - Database
=================================

This Java library is part of the **Urban Data Integration** project. It provides classes and functionality to maintain and transform (open urban) data sets.

Compute Database Column Similarity
----------------------------------

The library contains JAR files to compute pairwise similarity between database columns. Computation assumes that a unique term index has been generated a-priori.

### Create Unique Term Index

The JAR file `TermIndexGenerator.jar` is used to generate a set of unique terms in the database. Each term is assigned a unique identifier. With each term the index maintains the assigned data type and a comma-separated list of column:frequency pairs. Each pair denotes the frequency of the term in the identified column.

The possible data types that a term can have are:

1: INTEGER
2: DECIMAL
3: LONG
4: DATE
5: STRING

```
java -jar TermIndexGenerator.jar
  <input-directory> : Directory with column files
  <mem-buffer-size> : Size of the memory-buffer. Once buffer is full intermediate results are written to disk.
  <output-file>     : Output file for term index
```



### Compute Pairwise Column Similarity

```
java -jar ComputeColumnSimilarity.jar
  <term-index-file>     : The term-index file generated using TermIndexGenerator.jar
  <similarity-function> : Similarity function [
                            JI          : Jaccard-Index |
                            WJI-COLSIZE : Weighted Jaccard-Index that uses total number of values in a column as normalization scale |
                            WJI-COLMAX  : Weighted Jaccard-Index that uses the most frequent value in a column as normalization scale
                          ]
  <similarity-threshold>: Outputs only column pairs with similarity above the given threshold
  <threads>             : Number of parallel threads to use
  <output-file>         : Output file for similarities. The format is tab-delimited:
                          1) ID of column 1,
                          2) ID of column 2,
                          3) similarity
```



### N-Gram Column Generator

When computing column similarity based on n-grams one first has to transform the database column files into n-gram column files. These files only contain the n-grams for all column values. One then has to use `TermIndexGenerator.jar` to create a unique n-gram index before column similarity can be computed.

```
java -jar NGramColumnGenerator.jar
  <input-directory>      : Directory with database column files
  <ngram-size>           : Size of the generated n-grams
  <pad-for-short-values> : Add special padding characters ('$' and '#') at beginning and end of each value [true | false]
  <output-directory>     : Output directory for n-gram column files. The file format is the same as for the input files
```
