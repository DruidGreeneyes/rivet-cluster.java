###This requires Spark 1.6.0 and HBase 1.1.3 at the moment.

Setting up Spark and HBase on your local computer for testing should be easy; download binary versions, unpack them to their own folders, go in a do a little config according to their respective guides, and you should be up and running in no time. All you have to do then is feed your master url (spark://your-hostname:7077) to rosie.sh and it ought to work fine.

Getting it going on a more production oriented scale is still a work in progress; obviously I don't have a random hardware cluster lying around on my desk at home, so I've been working through AWS. This took awhile to figure out, but isn't actually that hard to -do-, provided you avoid a couple of pitfalls that neither Spark nor HBase currently mention. I assume you already know or can figure out most of AWS on your own. AMIs, instance types, IAM settings, all that crap. If you don't know what any of that is, set up a toy AWS instance and mess around until you get the hang of it. Then come back here.

#####First.

When choosing an instance type to deploy this on, it is both tempting and utterly reasonable to deploy on a single machine first before trying to scale to many. If you choose to do this, GET AN INSTANCE TYPE WITH MORE THAN TWO CORES. You need at least one core for each spark Worker and at least one core for the Master, so in theory two cores should work fine, but in practice the master tries to kill a worker and start a new worker at the same time, and when there's only one core to do this on, it seems to just crash and fail, at least in Spark 1.6.0. Later versions may fix this, and I may update my code to run on them, but it's not much of a priority atm, so don't hold your breath.

#####Second.

You need Java 8. If it's past April 2016, you should be able to use the Ubuntu 16.04 image and you will be fine. Otherwise, either use the Amazon Linux AMI or be prepared to jump through a lot of weird hoops in order to get Java 8 installed on Trusty.

