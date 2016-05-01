#Rivet.java

Cluster-backed Random-Index Vectoring in Java. Written in the voice of Mister Torgue.
This works on my laptop and on AWS, but should currently be treated as non-functional unless you want to dig through the source on your own and make it work for you.

##WHAT THE F$%K IS THIS!?!?

Random-Index Vectoring is a memory efficient way to perform textual analysis across corpora of extreme scale. It enables this by using contextual word co-occurrence as a stand-in for word meaning.

##WHAT THE HELL DOES THAT MEAN!?!?

Go read about it:

http://www.ercim.eu/publication/Ercim_News/enw50/sahlgren.html

If that doesn't give you enough of a primer, do the google. Sahlgren has done a lot of work on this and is a reliable source. So is Paradis.

##SOUNDS LIKE A JOYSPLOSION!! HOW CAN I GET IN ON THIS S&@T!?!?

Clone me.

Make sure you have passwordless ssh set up on your computer.

Install [HBase](https://hbase.apache.org) 1.2 and [Spark](https://spark.apache.org) 1.6.0. Read through the configuration docs and make sure everything is set up the way you want it to be.

Start hbase by running ```start-hbase``` in your hbase home directory

Start spark by running ```sbin/start-all.sh``` in your spark home directory

If you get errors in either case, go fix them. If both are running and you didn't manually change the default ports they use, you can find the spark webUI at https://localhost:8080, and you can find the hbase webUI at https://localhost:16010

Once everything is running, go into the cloned rivet directory and look in the conf folder. Copy all the *.template files and remove the .template suffix from your copies. Then edit them to suit you. You can find your Master URL by looking at the main page of spark's webUI. If you're running on AWS, keep in mind that the webUI will show the Internal DNS name, and unless you're running rivet from the master device of your cluster, you will need to use the External DNS instead.

Run rivet:

You can enter a primitive repl by running rosie.sh; from there you can get a list of available commands by typing ```ls``` and pressing enter.

If you know the command you want, you can run it and automatically exit the repl by passing it as arguments to rosie.sh

##I sense an unspoken 'but...'. SPIT IT OUT, A@#$%LE!!!

It's not really complete. Currently you can do two things with it; you can train a lexicon of words against a collection of text (a directory full of text files, not .doc files or .pdfs or any of that crap. .txt or fail.), and if you have a trained lexicon of words you can build a lexicon of topics by training it against a collection of .txt documents formatted as follows:

```xml
<topics>
topic1
topic2
topic3
...
topicN
</topics>
...
<body>
text of document
</body>
```

Using this at the scale for which it is intended will require a full-scale cluster, running Hadoop HDFS, Zookeeper, HBase 1.1.4, and Spark 1.6.0.
