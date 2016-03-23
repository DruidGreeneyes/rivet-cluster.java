##Getting Spark and HBase running on AWS.

This is a work in progress; I'm nearly done, but still have to work out some kinks with memory allocation. This document gets you as far as I've gotten, though.

###General Guidelines

Before you even start, you need at least one Amazon EC2 instance. You may at some point notice that A) Spark has a script that will start machine instances and set them up for you, and/or that B) Amazon has preconfigured clusters with spark installed. This would be fine if we were just using spark, but HBase throws a minor wrench in the gears. The machines that spark's `spark-ec2` script starts work, but the script assumes that spark is the only thing running, and so eats all the resources. Admittedly I didn't devote a huge amount of time to it, but I wasn't able to change the memory allocation through any logical means (i.e. editing the spark-env.sh files on the master and workers). As for Amazon's pre-configured clusters, having spark pre-installed is great, but it wasn't clear how to log into individual nodes and go about installing hbase alongside. Again, I didn't devote an enormous quantity of time to this; honestly, it's just more frustrating than interesting, and I'd really rather spend time on the program logic than on the infrastructure. If you get this running using either of the previous methods, let me know what you did and I'll be happy to update this guide.

In general, when selecting an instance or instances, there are two things you really need to pay attention to; vCPUs and RAM. More of both is obviously better, but comes at a price, so if you're trying to get started for cheap, here are the bare minima:

1. In a single-machine set-up, you need more than two cores. In a multi-instance set-up, each node needs 2 cores minimum (one for spark's worker or master, and one for everything else, which is mostly hbase), and it's best practice to use an odd number of nodes, both for spark and for zookeeper.

2. The workers seem to need more memory than they really ought to; this may be due to a misconfiguration or poor/failed partitioning on my part, but I'm not going to commit to that until I know for sure. What do know for now is that a worker with <=2G of ram tends to fail, and a worker with >=3G seems to work fine; this is still unconfirmed on a multi-instance setup. If you're running through this on your laptop, these limits do not seem to apply.

3. This really is much easier if you use Elastic IPs. It's another expense, albeit a minor one, and you can do this without, but using elastic IPs for your machines will save you a headache whenever you stop/restart your machine/cluster.

4. This guide does not use Hadoop or HDFS, but adding Hadoop/HDFS to these steps ought not to be difficult. If you do and you care, let me know what you did and I'll add a section.

###Single Machine

This has been tested on the c3.xlarge and the m2.xlarge instance types. c3.large has 4 cores and 7.5G ram, and works fine (2 workers @ 38M data processed in ~15 minutes). m2.xlarge has 2 cores and 17G ram and does not work.

Start your instance, do any necessary updates, and make sure java 8 is installed. 

Set up passwordless ssh; read this: http://www.tecmint.com/ssh-passwordless-login-using-ssh-keygen-in-5-easy-steps/ or google it yourself. To confirm it works, you should be able to ssh from the machine into the machine without supplying a key or a password: `ssh [username]@ip111-numbers-blah-numbers.ec2.internal` should give you no errors.

Use your preferred commandline downloading tool (wget or curl) to download hbase 1.1.3 and spark 1.6.0, which (unless this guide is absurdly out of date, in which case either the project has move to a different website or you probably shouldn't be using it) you can find by clicking around the download sections of both websites (hbase.apache.org, spark.apache.org).

`tar -xvf` both files, they'll generate their own folders.

Navigate to `[hbase-folder]/conf` and edit `hbase-site.xml`. Add the following, replacing items in [] with your own data:


```
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>file:///home/[username]/.hbase</value>
  </property>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>[The amazon internal DNS name of your instance, which will look something like this: ip-172-31-62-39.ec2.internal]</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/home/[username]/.zookeeper</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>
</configuration>
```

Edit the `regionservers` file and change `localhost` to the internal DNS name you just used above.

Edit hbase-env.sh; where it says `# Configure PermSize. Only needed in JDK7. You can safely remove it for JDK8+`, comment out the next two lines. This is bash, `#` is your comment character. Where it says `# The directory where pid files are stored. /tmp by default.`, uncomment the following line and change it to something that exists. I usually use `/path/to/hbase/dir/pids` and then I go create the `pids` folder.

Navigate to [spark-dir]/conf and make copies of `slaves.template`, and `spark-env.sh.template`. Rename the copies so they aren't templates.

Edit `slaves` and make the same change you made to `regionservers`.

Edit `spark-env.sh`. Most of it is pretty self-explanatory; the only things you -need- to change if you're just following this guide are:

```
SPARK_WORKER_CORES=1
SPARK_WORKER_MEMORY=[enough so that this line multiplied by SPARK_WORKER_INSTANCES equals out to less than your total available ram]
SPARK_WORKER_INSTANCES=[one or two less than the number of cores you have available]
```

I generally add these as new lines rather than uncommenting and changing the existing ones, so that I can come back and have a quick reminder of what they do.

The barebones configuration is complete; you can change some things in spark-defaults.conf or log4j.properties, but that isn't *necessary* for this to run. You can find a reference for those at spark's configuration page: https://spark.apache.org/docs/latest/configuration.html

You can also change other things in the spark or hbase configs to suit your needs/environment, again walking through the hbase or other appropriate documentation as needed.

In your AWS EC2 management console, select the instance you're working on. Under the Description tab, where it says "Security groups:", click on the link, which will probably be all but incomprehensible. You need to alter the security group settings so that incoming connections are allowed on (at the very least) the master's port and rest port. (7077 and 6066 by default) these will be Custom TCP rules at that port. If you're doing this for test purposes, it's fine to just leave it open to everywhere, but for a more secure set up you should probably limit it to the ips of the cluster plus other ip addresses you know and can trust. You can also open ports at 8080, 8081, and 4040 if you want to be able to access various features of spark's webui, and 16010 if you want to access HBase's webui.

start hbase by running `[hbase-folder]/bin/start-hbase.sh`
start spark by running `[spark-folder]/sbin/start-all.sh`

###Multiple Machines

Start one of whatever instance you want to use for this, and do the single-machine set-up, with the following differences; 

Everywhere that you reference one dns name (hbase/conf/regionservers and hbase-site.xml, spark/conf/slaves) you will instead reference the dns names of every machine you will eventually use as nodes in the cluster. How do you know what their dns names will be until you start them? You don't; for now, just add the name of this first node and move on.

That machine will be the master.

Go to your AWS EC2 management console and save this instance as an AMI. Once that process has finished, you can select it and click "Launch" until you have the number of workers you want. Make sure during the launch process that you set the security group of each new instance to the security group you changed above. You should be able to find it pretty easily. Each worker will now mostly be configured, but there are still a couple things you have to do:

The master needs to be able to ssh into all of the workers, so make sure you `cat` the master's `id_rsa.pub` into the `authorized_keys` of each worker.

Go back and edit those dns files from above (hbase/conf/regionservers and hbase-site.xml, spark/conf/slaves) on each worker, and make sure that each includes the entire list of private dns names for the master and all workers. Alternatively, edit each of those on the master and then copy it over to the workers. I dunno how to do this but I know it's possible and not particularly difficult. Uses `rsync` or something like that. Google it.

####You should now have a working setup.

If you get errors, go fix them; mostly you can google them without too much trouble, but here are a couple I had trouble with:

spark and/or hbase report problems communicating with nodes; this may manifest as zookeeper errors (can't find quorum at xxx.whatever.thing) or as spark errors (unable to communicate with worker at xxx.thing.whatever): make sure you're using the internal, or private, dns name in all the places where you told things where the nodes would be. 

spark and/or hbase report `connection refused (publickey)` when you try to start them: your passwordless ssh setup is wrong. Go double-check and fix it.

when I run rosie.sh, I get "All masters unresponsive, giving up" or something similar: check and make sure all the ports you need are open. If you changed the default port for your master or for the workers, you'll want to update your EC2 Security Group settings accordingly.
