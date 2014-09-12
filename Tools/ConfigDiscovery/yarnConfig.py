# Copyright (c) 2014, Cornell University
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the <organization> nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#!/usr/bin/python
import os, shlex, subprocess,time
import argparse


class ConfigDiscovery:
    def __init__(self):
        self.hadoop_home = os.environ["HADOOP_HOME"]
        self.PORT_NUMBER = 5000
        self.DEBUG = False
        self.local_rack_sum = 0
        self.tor2aggr = 0

        f = open('/etc/dsh/machines.list')  # read

        out = shlex.split(f)
        if self.DEBUG:
            print out

        # create folder TroikaConf to collect results from each node
        command = shlex.split("mkdir "+self.hadoop_home+"/TroikaConf")
        if self.DEBUG:
            print command

        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
        p.communicate()

        # reset the config Troika
        open(self.hadoop_home+"/TroikaConf/network_Troika.txt","w").close()

        # send benchmarking script to each node
        for host in out:
            command = shlex.split("rsync " +self.hadoop_home+"/clusterConfigReader.py "+host+":/mnt/yarn/hadoop-2.2.0")
            if self.DEBUG:
                print command
            p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
            out, err = p.communicate()
            if self.DEBUG:
                print out
        time.sleep(1)
        self.host = host

        # get input folder
        parser = argparse.ArgumentParser()

        parser.add_argument("appInputPath_HDFS", help="HDFS path of application input folder")
        parser.add_argument("diskDevicePath", help="Disk device path")
        parser.add_argument('rack_local_hosts', nargs='+', help='local host names (as appear in /etc/dsh/machines.list)')
        
        args = parser.parse_args()

        self.rack_local_hnames = args.rack_local_hosts      # rack local hosts for master node
        self.appInputPath_HDFS = args.appInputPath_HDFS     # appInputPath_HDFS
        self.diskDevicePath = args.diskDevicePath           # diskDevicePath

    def getInput(self):
        args = shlex.split("dsh -a -c -- python " + self.hadoop_home +"/clusterConfigReader.py " +self.appInputPath_HDFS + " " + self.diskDevicePath)
        if self.DEBUG:
            print args

        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
        out, err = p.communicate()
        if self.DEBUG:
            print out

    def measureBW(self):
        # measure bandwidth
        f = open('/etc/dsh/machines.list')  # read
        out = shlex.split(f)

        non_local_rack_sum = 0
        for host in out:
            if host != "localhost":
                command = shlex.split("dsh -m "+host+ " nttcp -i -p" + str(self.PORT_NUMBER))
                command_local = shlex.split("nttcp -T -p" +str(self.PORT_NUMBER)+" -n100000 "+host)
                command_kill = shlex.split("dsh -m "+host+ " killall nttcp")
                self.PORT_NUMBER += 1
                # remote (run in the background!!!)
                if self.DEBUG:
                    print command
                p = subprocess.Popen(command) # Success!

                time.sleep(1)

                #local
                if self.DEBUG:
                    print command_local
                p = subprocess.Popen(command_local, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
                out_local, err_local = p.communicate()
                out_local = shlex.split(out_local)

                if self.DEBUG:
                    print out_local
                bw = (float(out_local[13]) + float(out_local[21]))*64*1024  # in bytes

                # kill remote nttcp
                p = subprocess.Popen(command_kill, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
                out_kill, err_kill = p.communicate()

                if host in self.rack_local_hnames:
                    self.local_rack_sum += bw
                    print "Host: " + str(host) + " local BW: " + str(bw)
                else:
                    non_local_rack_sum += bw
                    print "Host: " + str(host) + " non_local BW: " + str(bw)

                with open(self.hadoop_home+"/TroikaConf/network_Troika.txt", "a") as myfile:
                    myfile.write("hostname:\t\t"+host+"\n" +str(bw)+"\n")


        self.local_rack_sum /= len(self.rack_local_hnames)
        non_local_rack_sum /= (len(out) - len(self.rack_local_hnames) - 1)

        print "----------------------"
        print "Local rack average    : " + str(self.local_rack_sum)
        print "Non-local rack average: " + str(non_local_rack_sum)

        self.tor2aggr = non_local_rack_sum/(2-(non_local_rack_sum/self.local_rack_sum))

        print "Node-to-TOR: " + str(self.local_rack_sum)
        print "TOR to Aggregate: " + str(self.tor2aggr)

    def transferBack(self):
        # send results back to master
        f = open('/etc/dsh/machines.list')  # read
        out = shlex.split(f)

        for host in out:
            command = shlex.split("rsync " + host +":"+self.hadoop_home+"/config_Troika.txt " + self.hadoop_home+ "/TroikaConf/config_Troika_" + host+ ".txt")
            if self.DEBUG:
                print command
            p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
            out, err = p.communicate()
            if self.DEBUG:
                print out

    def cleanUp(self):
        # remove output
        args = shlex.split("dsh -a -c -- rm " +self.hadoop_home+"/output")
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
        out, err = p.communicate()

        # remove config_Troika.txt
        args = shlex.split("dsh -a -c -- rm " +self.hadoop_home+"/config_Troika.txt")
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
        out, err = p.communicate()

        print "Please check your " + self.hadoop_home + "/TroikaConf folder for node properties and input.txt file"

    def generateIntro(self):
        # Write the common intro
        with open(self.hadoop_home+"/TroikaConf/input.txt", "w") as myfile:
            myfile.write("# AUTO GENERATED BY TROIKA DISCOVERY TOOL\n#\n# This is a comment\n# (commands are case sensitive)\n\
# application=Application, processor=CPU, hd=HardDisk, link=Link, memory=Memory,\n# node=Node, config=Configuration, switch=Switch, \
scheduler=Scheduler, filesplit=FileSplit Info\n# Configuration must come before applications\n# Filesplit order must be the same \
with the application order\n# \n# NOTE: Please make sure that filesplit locations are consistent with application size and related \
filesplit configs.\n# ____________________________________________________________________________________________________SYNTAX__\
_______________________________________\n# [] --> represents optional args. (with default values of Hadoop v2.2)\n#\n# APPLICATION:\
  application applicationSize(size_t) applicationOwnerID(size_t) mapIntensity(double) mapSortIntensity(double) reduceIntensity(double) \
reduceSortIntensity(double) mapOutputPercent(double)\n#               reduceOutputPercent(double) finalOutputPercent(double) \
clientEventID(int) rmEventID(int) queueId(size_t) [recordSize(size_t)\n#               [isThereACombiner(int) \
combinerIntensity(double) combinerCompressionPercent(double) [mapCpuVcores(int) reduceCpuVcores(int) \
[mapreduceMapMemory(size_t) mapreduceReduceMemory(size_t)\n#               [amResourceMB(size_t) amCpuVcores(int)]]]]]\n# \
CONFIG:       config mapreduceJobReduces(int) [mapreduceInputFileinputformatSplitMinsize(size_t) [mapreduceInputFileinputformat\
SplitMaxsize(size_t)\n#               [dfsBlocksize(size_t) [mapreducetaskIOSortFactor(int) [mapreduceTaskIOSortMb(size_t) \
[mapreduceMapSortSpillPercent(double)\n#               [mapreduceJobtrackerTaskscheduler(int) [mapreduceReduceShuffle\
Parallelcopies(int) [mapreduceReduceShuffleMergePercent(double)\n#               [mapreduceReduceShuffleInputBuffer\
Percent(double) [mapreduceReduceInputBufferPercent(double) [mapreduceJobUbertaskEnable(bool)\n#               [mapreduce\
JobUbertaskMaxmaps(int) [mapreduceJobUbertaskMaxreduces(int) \n#               [nodemanagerResourceMemoryMB(size_t) \
nodemanagerResourceCpuCores(int)\n#               [getMapreduceJobReduceSlowstartCompletedmaps(double) [mapred_child_\
java_opts(double)]]]]]]]]]]]]]]]]]\n# CPU:          processor numberOfCores(int) capacity(size_t) [delayType(int) [unit(int) \
[delayratio(double)]]]\n# MEMORY:       memory maxReadSpeed(size_t) maxWriteSpeed(size_t) minReadSpeed(size_t) minWriteSpeed(size_t)\
remainingCapacity(size_t)\n#               [delayType(int) [unit(int) [delayratio(double)]]]\n# HARDDISK:     hd maxRead\
Speed(size_t) maxWriteSpeed(size_t) minReadSpeed(size_t) minWriteSpeed(size_t) [delayType(int) [unit(int) [delayratio(double)]]]\n# \
NODE:         node ID(int) rackID(int) nodeType(int) connectedLinkID(int)\n# LINK:         link ID(int) \
capacity(double) upEndpointID(int) lowEndpointID(int) [mttr(double) [mtbf(double)\n#               [delayType(int) \
[unit(int) [delayratio(double)]]]]]\n# SWITCH:       switch ID(int) [upEndpointID(int)]\n# SCHEDULER:    scheduler number\
OfQueues(int) capacityOfQueue1(double) capacityOfQueue2(double) ... capacityOfQueueN(double) [maxAmResourcePercent(double) \
[minAllocMB(size_t) maxAllocMB(size_t)\n#               [minAllocVcores(int) maxAllocVcores(int) [resourceCalculator (bool)]]]]\n# \
FILESPLIT:    filesplit isforcedMapTaskCount(int) forcedReduceTaskCount(int) numberOfFilesplits(int) nodeExpectedId1(int) \
nodeExpectedId2(int) ... nodeExpectedIdN(int)\n# _________________________________________________________________________________\
___________________INPUT___________________________________________\n\n# Please replace each *value* with the corresponding value\n\
# E.g. *finalOutputPercent* might be replaced with 99\n# Remove the optional arguments that you would like to use the default values\
\n# Make sure that the parameter order is preserved and no undefined *value* remains in the final input.txt\n\n")

    def generateInput(self):
        # generate intro
        self.generateIntro()

        # THE DEFAULT VALUES FOR CONFIGS
        mapreduceJobReduces = "*mapreduceJobReduces*"
        mapreduceJobReduceSlowstartCompletedmaps = "*mapreduceJobReduceSlowstartCompletedmaps*"
        mapreduceMapSortSpillPercent = "*mapreduceMapSortSpillPercent*"
        mapreduceTaskIoSortFactor = "*mapreduceTaskIoSortFactor*"
        mapreduceTaskIoSortMb = "*mapreduceTaskIoSortMb*"
        mapreduceInputFileinputformatSplitMinsize = "*mapreduceInputFileinputformatSplitMinsize*"
        mapreduceInputFileinputformatSplitMaxsize = "*mapreduceInputFileinputformatSplitMaxsize*"
        dfsBlockSize = "*dfsBlockSize*"
        mapreduceReduceShuffleParallelcopies = "*mapreduceReduceShuffleParallelcopies*"
        mapreduceReduceShuffleMergePercent = "*mapreduceReduceShuffleMergePercent*"
        mapreduceReduceShuffleInputBufferPercent = "*mapreduceReduceShuffleInputBufferPercent*"
        mapreduceReduceInputBufferPercent = "*mapreduceReduceInputBufferPercent*"
        mapreduceJobUbertaskMaxmaps = "*mapreduceJobUbertaskMaxmaps*"
        mapreduceJobUbertaskMaxreduces = "*mapreduceJobUbertaskMaxreduces*"
        nodemanagerResourceMemoryMB = "*nodemanagerResourceMemoryMB*"
        nodemanagerResourceCpuCores = "*nodemanagerResourceCpuCores*"
        getMapreduceJobReduceSlowstartCompletedmaps = "*getMapreduceJobReduceSlowstartCompletedmaps*"
        mapred_child_java_opts = "*mapred_child_java_opts*"
        applicationSize = "*applicationSize*"
        numberOfLines = sum(1 for line in open(self.hadoop_home+"/TroikaConf/network_Troika.txt"))
        numberOfNodesInCluster = numberOfLines/2 + 1
        numberOfCores = "*numberOfCores*"
        remainingCapacity = "*remainingCapacity*"
        maxReadSpeed = "*maxReadSpeed*"
        maxWriteSpeed = "*maxWriteSpeed*"
        minReadSpeed = "*minReadSpeed*"
        minWriteSpeed = "*minWriteSpeed*"

        # Write SCHEDULER and SWITCHES
        with open(self.hadoop_home+"/TroikaConf/input.txt", "a") as myfile:
            myfile.write("# SCHEDULER\nscheduler 1 100\n\n# For each TOR Switch add the event id of each end.\n\n# TOR SWITCH (RACK_i)\nswitch *ID* *upEndpointID*\n\n\
# There is a single aggregation switch: add its event id.\n# AGGREGATION SWITCH (INTER RACK)\nswitch *expectedEventID*\n\n")


        # Get mapreduceJobReduces and applicationSize from self.hadoop_home+ "/TroikaConf/config_Troika_" + host+ ".txt" (any host)
        with open(self.hadoop_home+"/TroikaConf/config_Troika_" + self.host + ".txt") as f:
            content = f.readlines()

        for line in content:
            # a definition for mapreduceJobReduces is found
            if line.startswith('mapreduceJobReduces:'):
                 mapreduceJobReduces = line.split(None, 1)[1]
            elif line.startswith('mapreduceJobReduceSlowstartCompletedmaps:'):
                 mapreduceJobReduceSlowstartCompletedmaps = line.split(None, 1)[1]
            elif line.startswith('mapreduceMapSortSpillPercent:'):
                 mapreduceMapSortSpillPercent = line.split(None, 1)[1]
            elif line.startswith('mapreduceTaskIoSortFactor:'):
                 mapreduceTaskIoSortFactor = line.split(None, 1)[1]
            elif line.startswith('mapreduceTaskIoSortMb:'):
                 mapreduceTaskIoSortMb = line.split(None, 1)[1]
            elif line.startswith('mapreduceInputFileinputformatSplitMinsize:'):
                 mapreduceInputFileinputformatSplitMinsize = line.split(None, 1)[1]
            elif line.startswith('mapreduceInputFileinputformatSplitMaxsize:'):
                 mapreduceInputFileinputformatSplitMaxsize = line.split(None, 1)[1]
            elif line.startswith('dfsBlockSize:'):
                 dfsBlockSize = line.split(None, 1)[1]
            elif line.startswith('mapreduceReduceShuffleParallelcopies:'):
                 mapreduceReduceShuffleParallelcopies = line.split(None, 1)[1]
            elif line.startswith('mapreduceReduceShuffleMergePercent:'):
                 mapreduceReduceShuffleMergePercent = line.split(None, 1)[1]
            elif line.startswith('mapreduceReduceShuffleInputBufferPercent:'):
                 mapreduceReduceShuffleInputBufferPercent = line.split(None, 1)[1]
            elif line.startswith('mapreduceReduceInputBufferPercent:'):
                 mapreduceReduceInputBufferPercent = line.split(None, 1)[1]
            elif line.startswith('mapreduceJobUbertaskMaxmaps:'):
                 mapreduceJobUbertaskMaxmaps = line.split(None, 1)[1]
            elif line.startswith('mapreduceJobUbertaskMaxreduces:'):
                 mapreduceJobUbertaskMaxreduces = line.split(None, 1)[1]
            elif line.startswith('nodemanagerResourceMemoryMB:'):
                 nodemanagerResourceMemoryMB = line.split(None, 1)[1]
            elif line.startswith('nodemanagerResourceCpuCores:'):
                 nodemanagerResourceCpuCores = line.split(None, 1)[1]
            elif line.startswith('getMapreduceJobReduceSlowstartCompletedmaps:'):
                 getMapreduceJobReduceSlowstartCompletedmaps = line.split(None, 1)[1]
            elif line.startswith('mapred_child_java_opts:'):
                 mapred_child_java_opts = line.split(None, 1)[1]
            elif line.startswith('applicationSize'):
                 applicationSize = line.split(None, 1)[1]

        # Write CONFIGURATION and FILESPLIT locations
        with open(self.hadoop_home+"/TroikaConf/input.txt", "a") as myfile:
            myfile.write("# CONFIGURATION\nconfig "+ mapreduceJobReduces.rstrip()+ " [" + mapreduceInputFileinputformatSplitMinsize.rstrip() + " [" + mapreduceInputFileinputformatSplitMaxsize.rstrip()  + " [\
" + dfsBlockSize.rstrip() + " [" + mapreduceTaskIoSortFactor.rstrip() + " [" + mapreduceTaskIoSortMb.rstrip() + " [" + mapreduceMapSortSpillPercent.rstrip() + " [1 [" + mapreduceReduceShuffleParallelcopies.rstrip() + " [\
" + mapreduceReduceShuffleMergePercent.rstrip() + " [" + mapreduceReduceShuffleInputBufferPercent.rstrip() + " [" + mapreduceReduceInputBufferPercent.rstrip() + " [0 [" + mapreduceJobUbertaskMaxmaps.rstrip() + " [\
" + mapreduceJobUbertaskMaxreduces.rstrip() + " [" + nodemanagerResourceMemoryMB.rstrip() + " " + nodemanagerResourceCpuCores.rstrip() + " [" + getMapreduceJobReduceSlowstartCompletedmaps.rstrip() + " [\
" + mapred_child_java_opts.rstrip() +  "]]]]]]]]]]]]]]]]]\n\n# FILESPLIT LOCATIONS (in terms of expected node event type)\nfilesplit *isforcedMapTaskCount* *numberOfFilesplits* *nodeExpectedId1* \
*nodeExpectedId2* *...* nodeExpectedIdN*\n\n")


        # Write APPLICATION and FILESPLIT locations
        with open(self.hadoop_home+"/TroikaConf/input.txt", "a") as myfile:
            myfile.write("# APPLICATION\napplication "+ applicationSize.rstrip() +" 0 *mapIntensity* *mapSortIntensity* *reduceIntensity* *reduceSortIntensity* *mapOutputPercent* \
*reduceOutputPercent* *finalOutputPercent* *clientEventID* *rmEventID* 0\n\n# For each TOR to AGGR link add this\n# LINK (TOR to AGGR)\nlink *ID* "+ str(self.tor2aggr) + " \
*upEndpointID* *lowEndpointID*\n\n# Please update eventIDs of Node to TOR links\n")

        for _ in xrange(numberOfNodesInCluster):
            # Write Node to TOR links
            with open(self.hadoop_home+"/TroikaConf/input.txt", "a") as myfile:
                myfile.write("# LINK (to TOR)\nlink *ID* "+ str(self.local_rack_sum) + " *upEndpointID* *lowEndpointID*\n\n")

        f = open('/etc/dsh/machines.list')  # read
        out = shlex.split(f)

        # get each node info
        for host in out:
            # Get numberOfCores from self.hadoop_home+ "/TroikaConf/config_Troika_" + host+ ".txt"
            with open(self.hadoop_home+"/TroikaConf/config_Troika_" + host+ ".txt") as f:
                content = f.readlines()
            for line in content:
                if line.startswith('numberOfCores'):
                     numberOfCores = line.split(None, 1)[1]
                elif line.startswith('remainingMemCapacity'):
                     remainingCapacity = line.split(None, 1)[1]
                elif line.startswith('minReadSpeedInBytes'):
                     minReadSpeed = line.split(None, 1)[1]
                elif line.startswith('maxReadSpeedInBytes'):
                     maxReadSpeed = line.split(None, 1)[1]
                elif line.startswith('minWriteSpeedInBytes'):
                     minWriteSpeed = line.split(None, 1)[1]
                elif line.startswith('maxWriteSpeedInBytes'):
                     maxWriteSpeed = line.split(None, 1)[1]

            # Write Node configs
            with open(self.hadoop_home+"/TroikaConf/input.txt", "a") as myfile:
                myfile.write("# START: NODE_"+host+" CONFIGURATION\nprocessor "+numberOfCores+" *capacity*\n# MEMORY\nmemory *maxReadSpeed* \
*maxWriteSpeed* *minReadSpeed* *minWriteSpeed* "+remainingCapacity+"\n# max read/write speed - min read/write speed\n\
hd "+ maxReadSpeed.rstrip()+" "+maxWriteSpeed.rstrip()+" "+minReadSpeed.rstrip()+" "+minWriteSpeed.rstrip()+"\n# NODE\nnode *ID* \
*rackID* *nodeType* *connectedLinkID*\n# END: NODE_"+host+" CONFIGURATION\n\n")


config = ConfigDiscovery()
config.getInput()
config.measureBW()
config.transferBack()
config.cleanUp()
config.generateInput()