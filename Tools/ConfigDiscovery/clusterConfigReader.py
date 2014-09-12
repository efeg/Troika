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
import os, shlex, subprocess
import argparse
import xml.etree.ElementTree as ET

class ConfigReader:
    def __init__(self):
        self.CONST_MB_TO_BYTES = 1048576
        self.hadoop_home = os.environ["HADOOP_HOME"]
        self.hadoop_conf_dir = os.environ["HADOOP_CONF_DIR"]

        args = shlex.split("hostname")
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
        hname, err = p.communicate()

        print "[LOG] hostname " + hname,

        # reset the config Troika
        open(self.hadoop_home+"/config_Troika.txt","w").close()

        parser = argparse.ArgumentParser()
        parser.add_argument("hdfs_in_path", help="input folder path in hdfs")
        parser.add_argument("hd_path", help="the path in hard disk")

        args = parser.parse_args()
        hdfs_in_path = args.hdfs_in_path    # hdfs_in_path
        self.hd_path = args.hd_path         # hd_path

        args = shlex.split(self.hadoop_home + "/bin/hdfs dfs -du " + hdfs_in_path)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
        out, err = p.communicate()
        out = shlex.split(out)

        numlist = [int(x) for x in out[::2]]
        applicationSize =  sum(numlist)
        print "[LOG] applicationSize:\t\t\t" + str(applicationSize)

        with open(self.hadoop_home+"/config_Troika.txt", "a") as myfile:
            myfile.write("hostname:\t\t"+hname+"applicationSize:\t\t" + str(applicationSize))

        # params @ mapred-site.xml
        self.mapreduceJobReduces = "*mapreduceJobReduces*"
        self.mapreduceJobReduceSlowstartCompletedmaps = "*mapreduceJobReduceSlowstartCompletedmaps*"
        self.mapreduceMapSortSpillPercent = "*mapreduceMapSortSpillPercent*"
        self.mapreduceTaskIoSortFactor = "*mapreduceTaskIoSortFactor*"
        self.mapreduceTaskIoSortMb = "*mapreduceTaskIoSortMb*"
        self.mapreduceInputFileinputformatSplitMinsize = "*mapreduceInputFileinputformatSplitMinsize*"
        self.mapreduceInputFileinputformatSplitMaxsize = "*mapreduceInputFileinputformatSplitMaxsize*"
        self.mapreduceInputFileinputformatSplitMaxsize = "*mapreduceReduceShuffleParallelcopies*"
        self.mapreduceReduceShuffleMergePercent = "*mapreduceReduceShuffleMergePercent*"
        self.mapreduceReduceShuffleInputBufferPercent = "*mapreduceReduceShuffleInputBufferPercent*"
        self.mapreduceReduceInputBufferPercent = "*mapreduceReduceInputBufferPercent*"
        self.mapreduceJobUbertaskMaxmaps = "*mapreduceJobUbertaskMaxmaps*"
        self.mapreduceJobUbertaskMaxreduces = "*mapreduceJobUbertaskMaxreduces*"
        self.getMapreduceJobReduceSlowstartCompletedmaps = "*getMapreduceJobReduceSlowstartCompletedmaps*"
        self.mapred_child_java_opts = "*mapred_child_java_opts*"

        # params @ hdfs-site.xml
        self.dfsBlockSize = "*dfsBlockSize*"
        
        # params @ yarn-site.xml
        self.nodemanagerResourceMemoryMB = "*nodemanagerResourceMemoryMB*"
        self.nodemanagerResourceCpuCores = "*nodemanagerResourceCpuCores*"

    def parseMapRedSiteXML(self):
        # parse XML
        root = (ET.parse(self.hadoop_conf_dir + "/mapred-site.xml")).getroot()

        for p in root.findall('property'):
            if p.find('name').text == "mapreduce.job.reduces":
                self.mapreduceJobReduces = p.find('value').text
            elif p.find('name').text == "mapreduce.job.reduce.slowstart.completedmaps":
                self.mapreduceJobReduceSlowstartCompletedmaps = p.find('value').text
            elif p.find('name').text == "mapreduce.map.sort.spill.percent":
                self.mapreduceMapSortSpillPercent = p.find('value').text
            elif p.find('name').text == "mapreduce.task.io.sort.factor":
                self.mapreduceTaskIoSortFactor = p.find('value').text
            elif p.find('name').text == "mapreduce.task.io.sort.mb":
                self.mapreduceTaskIoSortMb = p.find('value').text
            elif p.find('name').text == "mapreduce.input.fileinputformat.split.minsize":
                self.mapreduceInputFileinputformatSplitMinsize = p.find('value').text
            elif p.find('name').text == "mapreduce.input.fileinputformat.split.maxsize":
                self.mapreduceInputFileinputformatSplitMaxsize = p.find('value').text
            elif p.find('name').text == "mapreduce.reduce.shuffle.parallelcopies":
                self.mapreduceInputFileinputformatSplitMaxsize = p.find('value').text
            elif p.find('name').text == "mapreduce.reduce.shuffle.merge.percent":
                self.mapreduceReduceShuffleMergePercent = p.find('value').text
            elif p.find('name').text == "mapreduce.reduce.shuffle.input.buffer.percent":
                self.mapreduceReduceShuffleInputBufferPercent = p.find('value').text
            elif p.find('name').text == "mapreduce.reduce.input.buffer.percent":
                self.mapreduceReduceInputBufferPercent = p.find('value').text
            elif p.find('name').text == "mapreduce.job.ubertask.maxmaps":
                self.mapreduceJobUbertaskMaxmaps = p.find('value').text
            elif p.find('name').text == "mapreduce.job.ubertask.maxreduces":
                self.mapreduceJobUbertaskMaxreduces = p.find('value').text
            elif p.find('name').text == "mapreduce.job.reduce.slowstart.completedmaps":
                self.getMapreduceJobReduceSlowstartCompletedmaps = p.find('value').text
            elif p.find('name').text == "mapred.child.java.opts":
                self.mapred_child_java_opts = p.find('value').text

        print "[LOG] mapreduceJobReduces:\t\t" + str(self.mapreduceJobReduces)
        print "[LOG] mapreduceJobReduceSlowstartCompletedmaps:\t" + str(self.mapreduceJobReduceSlowstartCompletedmaps)
        print "[LOG] mapreduceMapSortSpillPercent:\t\t" + str(self.mapreduceMapSortSpillPercent)
        print "[LOG] mapreduceTaskIoSortFactor:\t\t" + str(self.mapreduceTaskIoSortFactor)
        print "[LOG] mapreduceTaskIoSortMb:\t\t" + str(self.mapreduceTaskIoSortMb)
        print "[LOG] mapreduceInputFileinputformatSplitMinsize:\t\t" + str(self.mapreduceInputFileinputformatSplitMinsize)
        print "[LOG] mapreduceInputFileinputformatSplitMaxsize:\t\t" + str(self.mapreduceInputFileinputformatSplitMaxsize)
        print "[LOG] mapreduceReduceShuffleParallelcopies:\t\t" + str(self.mapreduceInputFileinputformatSplitMaxsize)
        print "[LOG] mapreduceReduceShuffleMergePercent:\t\t" + str(self.mapreduceReduceShuffleMergePercent)
        print "[LOG] mapreduceReduceShuffleInputBufferPercent:\t\t" + str(self.mapreduceReduceShuffleInputBufferPercent)
        print "[LOG] mapreduceReduceInputBufferPercent:\t\t" + str(self.mapreduceReduceInputBufferPercent)
        print "[LOG] mapreduceJobUbertaskMaxmaps:\t\t" + str(self.mapreduceJobUbertaskMaxmaps)
        print "[LOG] mapreduceJobUbertaskMaxreduces:\t\t" + str(self.mapreduceJobUbertaskMaxreduces)
        print "[LOG] getMapreduceJobReduceSlowstartCompletedmaps:\t\t" + str(self.getMapreduceJobReduceSlowstartCompletedmaps)
        print "[LOG] mapred_child_java_opts:\t\t" + str(self.mapred_child_java_opts)

    def parseHDFSSiteXML(self):
        # parse XML
        root2 = (ET.parse(self.hadoop_conf_dir + "/hdfs-site.xml")).getroot()

        for p in root2.findall('property'):
            if p.find('name').text == "dfs.blocksize":
                self.dfsBlockSize = p.find('value').text

        print "[LOG] dfsBlockSize:\t\t" + str(self.dfsBlockSize)

    def parseYARNSiteXML(self):
        # parse XML
        root3 = (ET.parse(self.hadoop_conf_dir + "/yarn-site.xml")).getroot()

        for p in root3.findall('property'):
            if p.find('name').text == "yarn.nodemanager.resource.memory-mb":
                self.nodemanagerResourceMemoryMB = p.find('value').text
            if p.find('name').text == "yarn.nodemanager.resource.cpu-vcores":
                self.nodemanagerResourceCpuCores = p.find('value').text

        print "[LOG] nodemanagerResourceMemoryMB:\t\t" + str(self.nodemanagerResourceMemoryMB)
        print "[LOG] nodemanagerResourceCpuCores:\t\t" + str(self.nodemanagerResourceCpuCores)

    def writeConfigs(self):

        self.parseMapRedSiteXML()
        self.parseHDFSSiteXML()
        self.parseYARNSiteXML()

        # Write Configs
        with open(self.hadoop_home+"/config_Troika.txt", "a") as myfile:
            myfile.write("\nmapreduceJobReduces:\t\t" + str(self.mapreduceJobReduces) + "\
\nmapreduceJobReduceSlowstartCompletedmaps:\t" + str(self.mapreduceJobReduceSlowstartCompletedmaps) + "\
\nmapreduceMapSortSpillPercent:\t" + str(self.mapreduceMapSortSpillPercent) + "\
\nmapreduceTaskIoSortFactor:\t" +str(self.mapreduceTaskIoSortFactor) + "\
\nmapreduceTaskIoSortMb:\t" + str(self.mapreduceTaskIoSortMb) + "\
\nmapreduceInputFileinputformatSplitMinsize:\t" + str(self.mapreduceInputFileinputformatSplitMinsize) + "\
\nmapreduceInputFileinputformatSplitMaxsize:\t" + str(self.mapreduceInputFileinputformatSplitMaxsize) + "\
\ndfsBlockSize:\t" + str(self.dfsBlockSize) + "\
\nmapreduceReduceShuffleParallelcopies:\t" + str(self.mapreduceInputFileinputformatSplitMaxsize) + "\
\nmapreduceReduceShuffleMergePercent:\t" + str(self.mapreduceReduceShuffleMergePercent) + "\
\nmapreduceReduceShuffleInputBufferPercent:\t" + str(self.mapreduceReduceShuffleInputBufferPercent) + "\
\nmapreduceReduceInputBufferPercent:\t" + str(self.mapreduceReduceInputBufferPercent) + "\
\nmapreduceJobUbertaskMaxmaps:\t" + str(self.mapreduceJobUbertaskMaxmaps) + "\
\nmapreduceJobUbertaskMaxreduces:\t" + str(self.mapreduceJobUbertaskMaxreduces) + "\
\ngetMapreduceJobReduceSlowstartCompletedmaps:\t" + str(self.getMapreduceJobReduceSlowstartCompletedmaps) + "\
\nmapred_child_java_opts:\t" + str(self.mapred_child_java_opts) + "\
\nnodemanagerResourceMemoryMB:\t" + str(self.nodemanagerResourceMemoryMB) + "\
\nnodemanagerResourceCpuCores:\t" + str(self.nodemanagerResourceCpuCores))

    def measureMem(self):
        # get total memory on the node (assume that the amount of memory is the same at each node but the user can modify config file by hand to change this...)
        args = shlex.split("free -bt")
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
        out, err = p.communicate()

        freeMem = out.split()
        memorySize = freeMem[freeMem.index("Total:") + 1]
        print "[LOG] remainingMemCapacity:\t\t" + memorySize

        with open(self.hadoop_home+"/config_Troika.txt", "a") as myfile:
            myfile.write("\nremainingMemCapacity:\t\t" + memorySize)

    def measureHD(self):
        # get cached reads and not cached reads
        args = shlex.split("sudo hdparm -Tt " + self.hd_path)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Success!
        out, err = p.communicate()

        hdData = out.split()
        cached = float(hdData[10])
        buffered = float(hdData[22])
        print "[LOG] cached read speed in MB:\t\t" + hdData[10] + "\nbuffered read speed in MB:\t\t" + hdData[22]

        minReadSpeed = (cached*0.01) + (buffered*0.99)
        maxReadSpeed = (cached*0.015) + (buffered*0.985)

        print "[LOG] min read speed in bytes:\t\t" + str(int(minReadSpeed*self.CONST_MB_TO_BYTES))
        print "[LOG] max read speed in bytes:\t\t" + str(int(maxReadSpeed*self.CONST_MB_TO_BYTES))

        with open(self.hadoop_home+"/config_Troika.txt", "a") as myfile:
            myfile.write("\ncachedReadSpeedInMB:\t" + hdData[10] + "\nbufferedReadSpeedInMB:\t" + hdData[22] + "\nminReadSpeedInBytes:\t\t" + str(int(minReadSpeed*self.CONST_MB_TO_BYTES)) + "\nmaxReadSpeedInBytes:\t\t" + str(int(maxReadSpeed*self.CONST_MB_TO_BYTES)))

        # get not cached write speed
        writeSpeed = 0
        maxWriteSpeed = 0
        for i in range(5):
            args = shlex.split("dd if=/dev/zero of="+self.hadoop_home+"/output conv=fdatasync bs=1M count=100")
            p = subprocess.Popen(args, stderr=subprocess.PIPE) # Success!
            _, out = p.communicate()

            hdData = out.split()
            writeSpeed += float(hdData[13])
            if float(hdData[13]) > maxWriteSpeed:
                maxWriteSpeed = float(hdData[13])

        writeSpeed /= 5

        print "[LOG] write speed in bytes:\t\t" + str(int(writeSpeed*self.CONST_MB_TO_BYTES))

        with open(self.hadoop_home+"/config_Troika.txt", "a") as myfile:
            myfile.write("\nminWriteSpeedInBytes:\t\t" + str(int(writeSpeed*self.CONST_MB_TO_BYTES)) + "\nmaxWriteSpeedInBytes:\t\t" + str(int(maxWriteSpeed*self.CONST_MB_TO_BYTES)))

    def measureCPU(self):
        # get number of physical cores

        args = shlex.split("grep '^core id' /proc/cpuinfo")
        p = subprocess.Popen(args, stdout=subprocess.PIPE) # Success!
        args = shlex.split("sort -u")
        p2 = subprocess.check_output(args, stdin=p.stdout)
        p.wait()
        ncores = p2.count('id')

        print "[LOG] numberOfCores:\t\t\t" + str(ncores)

        with open(self.hadoop_home+"/config_Troika.txt", "a") as myfile:
            myfile.write("\nnumberOfCores:\t\t" + str(ncores))


config = ConfigReader()
config.writeConfigs()
config.measureMem()
config.measureHD()
config.measureCPU()