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
1. [Apply patch to hadoop-1.0.1](http://wiki.apache.org/hadoop/HowToContribute)
2. Create express-hadoop.jar

  jar cf express-hadoop.jar src/express
3.  Add express-hadoop.jar to CLASSPATH
3. Recompile hadoop-1.0.1
ant -f build.xml compile

##How To##

##A Motivating Case##
**Hyperspectral data** is usually collected by sensors on an airborne or spaceborne platform. It is a valuable data source for many critical applications, such as mineral exploration, agricultural assessment, and special target recognition. Figure below shows a representative image of a hyperspectral cube. 

<figure>
  <img src="http://upload.wikimedia.org/wikipedia/en/4/48/HyperspectralCube.jpg" title="Graphic representation of hyperspectral data" alt="Graphic representation of hyperspectral data" height="160" width="160" />
  
  <br><figcaption><b>Graphic representation of hyperspectral data</b></figcaption>
</figure>

The image consists of two spatial dimensions and one spectral dimension. Terabytes of such data have been produced daily by EOS satellites since 1997. The accumulation of global hyperspectral datasets now reaches the petabytes scale. 

To analyze the data for a special purpose like geometric correction or mineral searching, the data needs to be partitioned regularly as the top cube shown in below Figure (a). The partitions then can be processed independently. MapReduce seems the proper solution at first, but two issues are readily apparent:

<figure>
  <a href="https://picasaweb.google.com/lh/photo/xvx5i6rLQwl2BNaZ4ps5pNMTjNZETYmyPJy0liipFm0?feat=embedwebsite"><img src="https://lh5.googleusercontent.com/-KE6-S-6Jq6M/T9Jo0BKGbbI/AAAAAAAAAAk/KlekTZmfBmE/s640/mot.png" title="Data Usage and Storage Partitioning" height="514" width="640" /></a>
  
  <br><figcaption><b>Data Usage and Storage Partitioning</b></figcaption>
</figure>

1. In traditional MapReduce, the data partitioning and distribution are not directly controlled by the user. So when the data usage pattern is illustrated by the top cube in Figure (b), the data may be actually partitioned as cubes in Figure (a).
 
2. Various usage patterns (partitioning) could be applied to the same chunk of data, depending on the analysis being performed. For instance, change detection tasks require broad spatial regions, and several adjacent spectral layers; signal processing tasks have no spatial region requirement, but a partition needs to contain all the spectral layers for one pixel. Figure (b) gives three possible usage patterns.

The storage-usage mismatch in Figure (a) and Figure (b) causes extra network traffic and synchronization. Figure (d) shows that in order to collect the red chunk of data for processing, nine blocks are accessed. Since data blocks are distributed over all nodes in the system, network latency variance and maximum bandwidth limitations could greatly slow down this data access. Due to the absence of data locality, the scalability of the map task stage degrades enormously in the scenario represented by Figure (d). When storage matches the data usage as described in Figure (e), data locality is preserved and the system becomes scalable again.





##The number of hashes determine the heading level## 

You can use “forbidden” HTML characters
like &, >, <, " and ', they are escaped
automatically. If you need additional HTML
formatting you can <span class="mySpan">just embed</span>
it into the Markdown source. You can include links
[easily](http://example.com "with a title") or 
[without a title](http://foo.example.com). If you use links often, 
[define them once][someid] and reference them by any id.
Just add the link definition anywhere, it will be removed 
from the output: 

[someid]: http://example.com "You can add a title"

You can add a horizontal ruler easily:

*******************************************

##Trailing hashes in a heading are optional

<div class="special"> If necessary you can even include
whole HTML blocks. Note however that Markdown code is *not*
evaluated inside HTML blocks, so if you want emphasis, you 
have to <em>add it yourself</em>    
</div>

You can embed verbatim code examples by indenting them by 
four spaces or a tab: 

    //This is verbatim code. Markdown syntax is *not*
    //processed here. However, HTML special chars are
    //escaped: < & > " '
    def foo() = <span>Hello World</span>

If you want verbatim code inline in your text
you can surround it with `def backticks():String`
or ``def doubleBackticks() = "To add ` to your code"``

* Unordered
* List items
* are started
* by asterisks

> To quote something, add ">" in front of 
> the quoted text        

1. Numbered Lists 
2. start with a number and a "."
234. the numbering is 
1. ignored in the output
250880. however and replaced
9. by consecutive numbers.

> To round things off
> 
> * you can 
> * nest lists
> 
> #headings#
> > and quotes
> > 
> >     //and code
> > 
> > ##as much as you want##
> > 
> > 1. also 
> >     * a list
> >     * in a list
> > 2. isn't that cool?