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
from threading import Lock, Semaphore
import re, os, time, sys
import argparse
import collections
from numpy import asmatrix, ones, matrix, array
import time

# python intensityOptimizer.py optimizerList.txt -b 50000 -l 1
# optimizerList.txt contains a list of bundles in the following example format:
# exp E1 res reducers 1 res mem 10737418240 res cores 2 res corespeed 840000000 res maxreadspeed 326548619 res maxwritespeed 326548619 res minreadspeed 98786585 res minwritespeed 98786585 res netspeed 125000000 repeat 2

class IntensityOptimizer:
    def __init__(self):
        # gets input file name and reads each line where the first character of the line is not '#' character 
        self.parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,

epilog="Each line of the experimental results file must be as follows:\n\
--------------------------------------------------------------\nexp \
<exp-name> res <conf-name_1> <conf_value_1> ... res <conf-name_m> \
<conf_value_m> <timing-name_1> <time_1 (s)> ... <timing-name_n> \
<time_n (s)> repeat <repetition-number>\n----------------------\
----------------------------------------\nNote: .* means any one or more consecutive characters\n\n\
ACCURACY-RELATED DATA\n\
---------------------\n\
repeat:\tnumber of times to repeat the simulation of this experiment\n\
\n\
TIMING-RELATED DATA <timing-name>\n\
---------------------------------\n\
elapsed:\telapsed time (s)\n\
avgMap:\t\taverage map time time (s)\n\
avgRed:\t\taverage reduce time time (s)\n\
shuffle:\taverage shuffle time time (s)\n\
\n\
CONFIGURATION-RELATED DATA <conf-name>\n\
--------------------------------------\n\
cores:\t\t\t\t\t\tnumber of cores\n\
coresp.*:\t\t\t\t\tcore speed in bytes/s\n\
nets.*:\t\t\t\t\t\tnetwork speed in bytes/s\n\
maxr.*:\t\t\t\t\t\tmaximum HD read speed in bytes/s\n\
maxw.*:\t\t\t\t\t\tmaximum HD write speed in bytes/s\n\
minr.*:\t\t\t\t\t\tminimum HD read speed in bytes/s\n\
minw.*:\t\t\t\t\t\tminimum HD write speed in bytes/s\n\
me.*:\t\t\t\t\t\tmemory size (bytes)\n\
app.*:\t\t\t\t\t\tapplication size (bytes)\n\
mapo.*:\t\t\t\t\t\tmap output percent\n\
reduceOutputPercent.*:\t\t\t\treduce output percent\n\
fin.*:\t\t\t\t\t\tfinal output percent\n\
mapreduce.input.fileinputformat.split.min.*:\tmapreduce.input.fileinputformat.split.min\n\
mapreduce.input.fileinputformat.split.max.*:\tmapreduce.input.fileinputformat.split.max\n\
dfs.*:\t\t\t\t\t\tdfs.blocksize\n\
mapreduce.map.sort.spill.percent.*:\t\tmapreduce.map.sort.spill.percent\n\
yarn.nodemanager.resource.memory-mb.*:\t\tyarn.nodemanager.resource.memory-mb\n\
mapreduce.task.io.sort.mb.*:\t\t\tmapreduce.task.io.sort.mb\n\
mapreduce.task.io.sort.factor.*:\t\tmapreduce.task.io.sort.factor\n\
mapreduce.reduce.shuffle.merge.percent.*:\tmapreduce.reduce.shuffle.merge.percent\n\
mapreduce.reduce.shuffle.input.buffer.percent.*:mapreduce.reduce.shuffle.input.buffer.percent\n\
mapreduce.job.reduce.slowstart.completedmaps.*:\tmapreduce.job.reduce.slowstart.completedmaps\n\
record.*:\t\t\t\t\trecord size (bytes)\n\
mapreduceMapMemory.*:\t\t\t\tmapreduceMapMemory\n\
mapreduceReduceMemory.*:\t\t\tmapreduceReduceMemory\n\
amResourceMB.*:\t\t\t\t\tmapreduce.am.resource.mb\n\
mapCpuVcores.*:\t\t\t\t\tmapreduce.map.cpu.vcores\n\
reduceCpuVcores.*:\t\t\t\tmapreduce.reduce.cpu.vcores\n\
mapIntensity.*:\t\t\t\t\tmapIntensity\n\
mapSortIntensity.*:\t\t\t\tmapSortIntensity\n\
reduceIntensity.*:\t\t\t\treduceIntensity\n\
reduceSortIntensity.*:\t\t\t\treduceSortIntensity\n\
combinerIntensity.*:\t\t\t\tcombinerIntensity\n\
reducer.*:\t\t\t\t\tnumber of reduce tasks\n\
")

        # required argument
        self.parser.add_argument(      "experimentFileName",        help="Filename of the input, containing the experiment results")
        self.parser.add_argument("-i", "--TROIKAInputFileName",     help="Input file to be used by TROIKA",                     default="input.txt")
        self.parser.add_argument("-e", "--executablePath",          help="Path to TROIKA's executable",           default="../../Debug/Troika")
        self.parser.add_argument("-l", "--loglevel",                help="Log level [0: none, 1: regular, 2: all]", type=int,   default=1)
        self.parser.add_argument("-a", "--accuracy",                help="Accuracy of Elapsed Time",                type=float, default=0.1)
        self.parser.add_argument("-r", "--retrial",                 help="Retrial count until accruacy is reached", type=int,   default=10)

        self.args = self.parser.parse_args()

        if self.args.accuracy < 0.01:
            sys.exit('Please make sure that accuracy is greater than or equal to 0.01')

        # start analysis for shortest time 
        with open(self.args.experimentFileName) as f:
            self.content = f.readlines()

        # contains the systems to be tested (the list is populated in "eliminate" fn)
        self._systemsToBeTested = []
        self._intensities = []
        self.numberOfConfigs = 0
        self.DEFAULT_SIMCOUNT = 5

    @property
    def systemsToBeTested(self):
        return self._systemsToBeTested
    
    # Used to detect if minimum memory capacity is less than the number set
    # in required amount in input config to run map-reduce tasks properly.
    def getNodemanagerResourceMemory(self):
        # read from filename
        TROIKAInput = open(self.args.TROIKAInputFileName,"r")

        for line in TROIKAInput:
            if line[0] == 'c':
                if len(line.split()) > 16:
                    TROIKAInput.close() 
                    return long(line[16])
                else:
                    # default value of yarn.nodemanager.resource.memory-mb (as of version 2.2.0 and 2.3.0)
                    TROIKAInput.close()
                    return 8192

    def getPhysicalMem(self):
        # read from filename
        TROIKAInput = open(self.args.TROIKAInputFileName,"r")

        for line in TROIKAInput:
            if line[0] == 'm':
                TROIKAInput.close()
                return long(line.split()[5])/1048576

    # eliminate systems with insufficient memory, higher cost than the limit
    # returns number of configs in the new list
    def eliminate(self):
        # insufficient memory satisfiability check
        requiredMemory = self.getNodemanagerResourceMemory()
        physicalMem = self.getPhysicalMem()

        for x in self.content:
            # a exp definition is found
            if x.startswith('exp'):
                
                lisOfWords = x.split()
                memIndex = -1
                nmResourceMemIndex = -1

                for word in lisOfWords:
                    # find memory (if provided)
                    if word.startswith('me'):
                        memIndex=lisOfWords.index(word) + 1
                        physicalMem = long(lisOfWords[memIndex])
                    # find yarn.nodemanager.resource.memory-mb (if provided)
                    elif word.startswith('yarn.nodemanager.resource.memory-mb'):
                        nmResourceMemIndex = lisOfWords.index(word) + 1
                        requiredMemory = long(lisOfWords[nmResourceMemIndex])
                    
                # check if memory is sufficient
                if (physicalMem >= requiredMemory):
                    self._systemsToBeTested.append(x)
      
        self.numberOfConfigs = len(self._systemsToBeTested)

        # sanity check for existence of at least one available bundle
        if self.numberOfConfigs == 0:
            sys.exit('Please check your input list and make sure that for at least one of the systems in it you have sufficient memory')

        return self.numberOfConfigs

    def extractCommand(self, system):

        expID       = system.split(None, 2)[1]
        regularCommand = ""
        totSimCount = self.DEFAULT_SIMCOUNT
        words = system.split()

        elapsed = 0
        avgMap = 0
        avgRed = 0
        shuffle = 0

        resCount = -1
        redIsReady = False

        for word in words:
            if word == "res":
                resCount = 2
            elif word.startswith('repeat'):
                totSimCount = long(words[words.index(word)+1])
            elif word.startswith('elapsed'):
                elapsed = long(words[words.index(word)+1])
            elif word.startswith('avgMap'):
                avgMap = long(words[words.index(word)+1])
            elif word.startswith('avgRed'):
                avgRed = long(words[words.index(word)+1])
            elif word.startswith('shuffle'):
                shuffle = long(words[words.index(word)+1])
            # detect resource names
            elif resCount == 1:

                if word == 'cores':
                    regularCommand += " -c "
                elif word.startswith('coresp'):
                    regularCommand += " -s "
                elif word.startswith('nets'):
                    regularCommand += " -l "
                elif word.startswith('maxr'):
                    regularCommand += " -v "
                elif word.startswith('maxw'):
                    regularCommand += " -w "
                elif word.startswith('minr'):
                    regularCommand += " -x "
                elif word.startswith('minw'):
                    regularCommand += " -y "
                elif word.startswith('me'):
                    regularCommand += " -m "
                elif word.startswith('app'):
                    regularCommand += " -a "
                elif word.startswith('mapo'):
                    regularCommand += " -p "
                elif word.startswith('reduceOutputPercent'):
                    regularCommand += " -u "
                elif word.startswith('fin'):
                    regularCommand += " -f "
                elif word.startswith('mapreduce.input.fileinputformat.split.min'):
                    regularCommand += " -b "
                elif word.startswith('mapreduce.input.fileinputformat.split.max'):
                    regularCommand += " -d "
                elif word.startswith('dfs'):
                    regularCommand += " -g "
                elif word.startswith('mapreduce.map.sort.spill.percent'):
                    regularCommand += " -h "
                elif word.startswith('yarn.nodemanager.resource.memory-mb'):
                    regularCommand += " -j "
                elif word.startswith('mapreduce.task.io.sort.mb'):
                    regularCommand += " -k "
                elif word.startswith('mapreduce.task.io.sort.factor'):
                    regularCommand += " -n "
                elif word.startswith('mapreduce.reduce.shuffle.merge.percent'):
                    regularCommand += " -o "
                elif word.startswith('mapreduce.reduce.shuffle.input.buffer.percent'):
                    regularCommand += " -z "
                elif word.startswith('mapreduce.job.reduce.slowstart.completedmaps'):
                    regularCommand += " -q "
                elif word.startswith('record'):
                    regularCommand += " --recordSize "
                elif word.startswith('mapreduceMapMemory'):
                    regularCommand += " --mapreduceMapMemory "
                elif word.startswith('mapreduceReduceMemory'):
                    regularCommand += " --mapreduceReduceMemory "
                elif word.startswith('amResourceMB'):
                    regularCommand += " --amResourceMB "
                elif word.startswith('mapCpuVcores'):
                    regularCommand += " --mapCpuVcores "
                elif word.startswith('reduceCpuVcores'):
                    regularCommand += " --reduceCpuVcores "
                elif word.startswith('mapIntensity'):
                    regularCommand += " --mapIntensity "
                elif word.startswith('mapSortIntensity'):
                    regularCommand += " --mapSortIntensity "
                elif word.startswith('reduceIntensity'):
                    regularCommand += " --reduceIntensity "
                elif word.startswith('reduceSortIntensity'):
                    regularCommand += " --reduceSortIntensity "
                elif word.startswith('combinerIntensity'):
                    regularCommand += " --combinerIntensity "
                elif word.startswith('reducer'):
                    regularCommand += " -r "
                    redIsReady = True

            # detect resource values 
            elif resCount == 0:
                regularCommand += str(word)
                if redIsReady:
                    redCount = str(word)
                    redIsReady = False

            # decrease resource counters
            resCount -= 1

        return regularCommand, redCount, expID, totSimCount, elapsed, avgMap, avgRed, shuffle

    # returns map, reduce and corresponding sort intensities
    # if there is a combiner returns its intensity.
    def getIntensities(self):
        # read from filename
        TROIKAInput = open(self.args.TROIKAInputFileName,"r")

        for line in TROIKAInput:
            if line[0] == 'a':
                combinerInt = 0
                if len(line.split()) > 15:
                    combinerInt = float(line[15])
                TROIKAInput.close()
                return float(line.split()[3]),float(line.split()[4]),float(line.split()[5]),float(line.split()[6]),combinerInt

    # perform the simulation with the given parameters using the TROIKA simulator
    # return the performance test result
    def performanceTest(self, command, redCount, expID, totSimCount, elapsed, avgMap, avgRed, shuffle):

        retrial = self.args.retrial

        # get map, reduce and corresponding sort intensities.
        # if there is a combiner, get its intensity.
        mapInt, mapSortInt, reduceInt, reduceSortInt, combinerInt = self.getIntensities()

        intensities = str(expID) + " map intensity: " + str(mapInt) + " map sort intensity: " + str(mapSortInt) + " reduce intensity: " + str(reduceInt) + " reduce sort intensity: " + str(reduceSortInt) + " combiner intensity: " + str(combinerInt)
        originalCommand = command

        while retrial != 0:
            for sim in xrange(totSimCount):

                if self.args.loglevel > 1:
                    print self.args.executablePath + command
                os.system(self.args.executablePath + command)
                if self.args.loglevel > 0:
                    print "[LOG]: Progress: " +  str(sim+1) + "/" + str(totSimCount) +" of the simulation of system with ID: " + str(expID) + " is finished."

            time.sleep(1)
            # note: this must be reducer...
            with open("test" + redCount + ".txt") as f:
                content = f.readlines()

            os.system("rm test" + redCount + ".txt")

            ElapsedTotal = 0
            AvgMap = 0
            AvgReduce = 0
            AvgShuffle = 0
            TotalMap = 0

            for i in xrange(totSimCount):
                ElapsedTotal += float(content[0 +5*i])
                AvgMap += float(content[1 +5*i])
                AvgReduce += float(content[2 +5*i])
                AvgShuffle += float(content[3 +5*i])
                TotalMap += float(content[4 +5*i])

            toBeWritten = str(expID) + " " +str(ElapsedTotal/totSimCount) + " " + str(AvgMap/totSimCount) + " " + str(AvgReduce/totSimCount) + " " + str(AvgShuffle/totSimCount) + " " + str(TotalMap/totSimCount) + "\n"

            currentAccuracy = abs((ElapsedTotal/totSimCount) - elapsed)/(ElapsedTotal/totSimCount)

            # if within the accuracy break
            if currentAccuracy < self.args.accuracy:
                break

            # otherwise try finding intensities within the accuracy
            else:
                retrial -= 1
                # detect the largest difference (is it in map intensity or reduce intensity?)

                redDif = 0
                mapDif = 0

                if (AvgReduce/totSimCount) != 0:
                    redDif = avgRed-(AvgReduce/totSimCount) / (AvgReduce/totSimCount)
                if (AvgMap/totSimCount) != 0:
                    mapDif = avgMap-(AvgMap/totSimCount) / (AvgMap/totSimCount)

                if abs(mapDif) > abs(redDif):
                    # change the map related config to observe the effect of its change
                    
                    mapInt = (elapsed * mapInt)/(ElapsedTotal/totSimCount)
                    mapSortInt = mapInt

                else:
                    # change the reduce related config to observe the effect of its change
                    reduceInt = (elapsed * reduceInt)/(ElapsedTotal/totSimCount)
                    reduceSortInt = reduceInt

                    # if there is a combiner also change its intensity accordingly
                    if combinerInt != 0:
                        combinerInt = reduceInt


                command = originalCommand + " --mapIntensity " + str(mapInt) + " --mapSortIntensity " + str(mapSortInt) + " --reduceIntensity " + str(reduceInt) + " --reduceSortIntensity " + str(reduceSortInt) + " --combinerIntensity " + str(combinerInt)

            intensities = str(expID) + " map intensity: " + str(mapInt) + " map sort intensity: " + str(mapSortInt) + " reduce intensity: " + str(reduceInt) + " reduce sort intensity: " + str(reduceSortInt) + " combiner intensity: " + str(combinerInt)

        return toBeWritten, intensities

    def tester(self):

        for expID in xrange(self.numberOfConfigs):
            if self.args.loglevel > 0:
                print("[LOG]: Start simulation for expID: " + str(self._systemsToBeTested[expID].split(None, 2)[1]))

            regularCommand, redCount, expID, totSimCount, elapsed, avgMap, avgRed, shuffle = self.extractCommand(self._systemsToBeTested[expID])
            if self.args.loglevel > 1:
                print "regCommand " + str(regularCommand) + " redCount" + redCount + " expID " + str(expID) + " totSimCount " + str(totSimCount) + " elapsed " + str(elapsed) + " avgMap " + str(avgMap) + " avgRed " + str(avgRed) + " shuffle " + str(shuffle)

            # perform the test 
            toBeWritten, bestCommand = self.performanceTest(regularCommand, redCount, expID, totSimCount, elapsed, avgMap, avgRed, shuffle)

            self._intensities.append(bestCommand)

            if self.args.loglevel > 0:
                print("[LOG]: Finish simulation for expID: " + str(expID))

    def presentResult(self):

        print "----------------------------------\nRECOMMENDED INTENSITIES:"

        for intensity in self._intensities:
            print intensity


start_time = time.time()

greedy = IntensityOptimizer()
numberOfConfigs = greedy.eliminate()
greedy.tester()

print time.time() - start_time, "seconds"

greedy.presentResult()