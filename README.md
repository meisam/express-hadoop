EXPRESS-HADOOP
==============

A Hadoop extension to support high dimensional data analysis

##Introduction##
EXPRESS is proposed to enable efficient processing of high-dimensional scientific data. It takes advantage of prior knowledge in **data structure** and **data usage pattern**. By performing **incongruent data partitioning** and **locality aware task scheduling**, EXPRESS effectively reduces the network traffic and task execution time. Highlighted features involve:

* User interfaces to describe and use data in a **structure-aware** language. 
* A novel **incongruent data partitioning** scheme for replicas. EXPRESS supports the coexistence of multiple data partitioning for the same data. It also introduces a set of optimizations to fully realize the potential of incongruent partitioning.
* Data **layout aware** task selection and scheduling. By exposing data layout information, the EXPRESS scheduler collocates the map/reduce tasks with related data. When data layout matches its usage pattern, EXPRESS can select the proper map task to accelerate the data loading.

##Installation##
###Prerequirement  
hadoop-1.0.1, ant, patch

###Steps
1. [Apply express patch to hadoop-1.0.1](http://wiki.apache.org/hadoop/HowToContribute)
2. Create express-hadoop.jar

    ``jar cf express-hadoop.jar src/express``
  
3. Add express-hadoop.jar to CLASSPATH
3. Recompile hadoop-1.0.1

    ``ant -f build.xml compile``

##How To##
* Use express.hdd.HDFGen to generate test data with specifc partitioning scheme
    
    ``bin/hadoop jar express-hadoop.jar hdf.test.HDFGen [dataSize] [partitionOffset] [recordSize] [partitionSize] [outDir]``

* Use express.hdd.HDFMicroBenchmark to load data with specific pattern

    ``bin/hadoop jar express-hadoop.jar hdf.test.HDFMicroBenchmark [dataSize] [chunkOffset] [chunkSize] [inDir] [outDir]``

* run tests/validate.sh for validation

##A Motivating Case##
**Hyperspectral data** is usually collected by sensors on an airborne or spaceborne platform. It is a valuable data source for many critical applications, such as mineral exploration, agricultural assessment, and special target recognition. Figure below shows a representative image of a hyperspectral cube. 

<figure>
  <img src="http://upload.wikimedia.org/wikipedia/en/4/48/HyperspectralCube.jpg" title="Graphic representation of hyperspectral data" alt="Graphic representation of hyperspectral data" height="160" width="160" />
  
  <br><figcaption><b>Figure 1</b> Graphic representation of hyperspectral data</figcaption>
</figure>

The image consists of two spatial dimensions and one spectral dimension. Terabytes of such data have been produced daily by EOS satellites since 1997. The accumulation of global hyperspectral datasets now reaches the petabytes scale. 

To analyze the data for a special purpose like geometric correction or mineral searching, the data needs to be partitioned regularly as the top cube shown in below Figure (a). The partitions then can be processed independently. MapReduce seems the proper solution at first, but two issues are readily apparent:

<figure>
  <a href="https://picasaweb.google.com/lh/photo/xvx5i6rLQwl2BNaZ4ps5pNMTjNZETYmyPJy0liipFm0?feat=embedwebsite"><img src="https://lh5.googleusercontent.com/-KE6-S-6Jq6M/T9Jo0BKGbbI/AAAAAAAAAAk/KlekTZmfBmE/s640/mot.png" title="Data Usage and Storage Partitioning" height="514" width="640" /></a>
  
  <br><figcaption><b>Figure 2</b> Data Usage and Storage Partitioning</figcaption>
</figure>

1. In traditional MapReduce, the data partitioning and distribution are not directly controlled by the user. So when the data usage pattern is illustrated by the top cube in Figure 2(b), the data may be actually partitioned as cubes in Figure 2(a).
 
2. Various usage patterns (partitioning) could be applied to the same chunk of data, depending on the analysis being performed. For instance, change detection tasks require broad spatial regions, and several adjacent spectral layers; signal processing tasks have no spatial region requirement, but a partition needs to contain all the spectral layers for one pixel. Figure 2(b) gives three possible usage patterns.

The storage-usage mismatch in Figure 2(a) and Figure 2(b) causes extra network traffic and synchronization. Figure 2(d) shows that in order to collect the red chunk of data for processing, nine blocks are accessed. Since data blocks are distributed over all nodes in the system, network latency variance and maximum bandwidth limitations could greatly slow down this data access. Due to the absence of data locality, the scalability of the map task stage degrades enormously in the scenario represented by Figure 2(d). When storage matches the data usage as described in Figure 2(e), data locality is preserved and the system becomes scalable again.

##Design##

##Authors##
* Code: [Siyuan Ma](http://siyuan.biz)

<a href="https://github.com/s1van/express-hadoop"><img style="position: absolute; top: 0; right: 0; border: 0;" src="https://s3.amazonaws.com/github/ribbons/forkme_right_green_007200.png" alt="Fork me on GitHub"></a>

