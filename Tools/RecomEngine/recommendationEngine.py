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
from numpy import asmatrix, ones, matrix, array, linalg, set_printoptions, nan
import time

set_printoptions(threshold=nan)

# python recommendationEngine.py bundles.txt -b 50000 -l 1
# bundles.txt contains a list of bundles in the following example format:
# bundle M1.large rangeres reducers 1 2 1 res mem 10737418240 res cores 2 res corespeed 840000000 res maxreadspeed 326548619 res maxwritespeed 326548619 res minreadspeed 98786585 res minwritespeed 98786585 res netspeed 125000000 nodecost 2000 linkcost 0 repeat 2

# if -z parameter is provided as input, then the program runs using multi-dimensional optimization
# if -z parameter is NOT provided AND the number of bundles within the cost and memory limits are at least (#res+1), then FIRST ORDER RSM is used


class RecommendationEngine:
    def __init__(self):
        # gets input file name and reads each line where the first character of the line is not '#' character 

        self.parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,

epilog="Each line of the configuration space must be as follows:\n\
--------------------------------------------------------------\nbundle \
<bundle-name> rangeres reducers <bottom limit> <upper limit> <step size> \
res <conf-name_1> <conf_value_1> ... res <conf-name_m> <conf_value_m> \
nodecost <cost_value> linkcost <cost_value> repeat <repetition-number>\n\
--------------------------------------------------------------\nNote: .* \
means any one or more consecutive characters\n\n\
ACCURACY-RELATED DATA\n\
---------------------\n\
repeat:\tnumber of times to repeat the simulation of this bundle configuration\n\
\n\
CONFIGURATION-RELATED DATA <conf-name>\n\
--------------------------------------\n\
cores:\t\t\t\t\t\tnumber of cores\n\
coresp.*:\t\t\t\t\tcore speed\n\
nets.*:\t\t\t\t\t\tnetwork speed in bytes/s\n\
maxr.*:\t\t\t\t\t\tmaximum HD read speed\n\
maxw.*:\t\t\t\t\t\tmaximum HD write speed\n\
minr.*:\t\t\t\t\t\tminimum HD read speed\n\
minw.*:\t\t\t\t\t\tminimum HD write speed\n\
me.*:\t\t\t\t\t\tmemory size\n\
app.*:\t\t\t\t\t\tapplication input size\n\
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
record.*:\t\t\t\t\trecord size\n\
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
queueIDs.*:\t\t\t\tCapacityScheduler queueIDs for applications\n\
schQueues.*:\t\t\t\tCorresponding capacity of queues\n\
")

        # required argument
        self.parser.add_argument(      "configFileName",        help="Filename of the input, containing the bundles")
        # optional arguments
        self.parser.add_argument("-i", "--TROIKAInputFileName", help="Input file to be used by TROIKA",           default="input.txt")
        self.parser.add_argument("-e", "--executablePath",      help="Path to TROIKA's executable",           default="../../Debug/Troika")
        self.parser.add_argument("-b", "--budget",              help="The total cluster budget limit", type=long, default=50000)
        self.parser.add_argument("-z", "--testTolerance",       help="Tolerance percentage to worse performance", type=float)
        self.parser.add_argument("-l", "--loglevel",            help="Log level [0: none, 1: regular, 2: all, 3: debug]", type=int, default=1)
        self.parser.add_argument("-s", "--samplesize",          help="Samples per parameter(try to provide 1:20 ratio)", type=int, default=5)
        self.parser.add_argument("-f", "--finaltestcount",      help="Systems to be tested after fitting a model", type=int, default=3)
        self.parser.add_argument("-o", "--rsmModelOrder",       help="[1: first order model, 2: second order model]", type=int, default=2)
        self.parser.add_argument("-d", "--debugMode",           help="[0: deactivate 1: activate]", type=int, default=0)

        self.args = self.parser.parse_args()
        # start analysis for shortest time 
        with open(self.args.configFileName) as f:
            self.content = f.readlines()

        self.useRSM = True
        if self.args.testTolerance:
            self.useRSM = False
            # check whether test tolerance is within the limits
            if not (self.args.testTolerance >= 0):
                sys.exit('Please make sure that input test tolerance is positive')
        else:
            # default value if no value was provided this will be used
            self.args.testTolerance = 0.1

        if self.args.samplesize < 5:
            print "For sample size less than 5, RSM cannot be used. Fallback to multidimensional optimization."
            self.useRSM = False

        # hash table to be used as a cache for already completed computations.
        # (key = simID, value = performance result + corresponding configuration)
        self.resultCache = {}
        # a mapping for simId --> cluster cost
        self.simID_clusterCost = {}

        # a mapping for presenting parameter values
        self.paramFlag_value = {}

        self.DEFAULT_SIMCOUNT = 5
        # default value of yarn.nodemanager.resource.memory-mb (as of version 2.2.0 and 2.3.0)
        self.DEFAULT_RESOURCEMEMSIZE = 8192
        self.SAMPLES_PER_PARAM = self.args.samplesize
        self.RSM_MODEL = self.args.rsmModelOrder
        # how many systems will be tested after fitting a model and finding the expected best configs?
        self.FINAL_TEST_COUNT = self.args.finaltestcount

        # the best perf result and corresponding configuration result found so far
        self.bestPerformance = "0 " + str(sys.float_info.max) + " 0 0 0 0"
        self.bestCommand = ""
        self.numberOfResources = 0
        self.numberOfBundles = 0

        # contains the systems to be tested (the list is populated in "eliminate" fn)
        self._systemsToBeTested = []
        self.sortedSystems = []

        # options that are equal to certain string
        self.options_equal_list = [('cores', '-c')]
        self.options_equal_dict = {key: value for key, value in self.options_equal_list}

        # options that start with certain string
        self.options_startswith_list = [('coresp', ' -s'),
                         ('nets', '-l'),
                         ('maxr', '-v'),
                         ('maxw', '-w'),
                         ('minr', '-x'),
                         ('minw', '-y'),
                         ('me', '-m'),
                         ('app', '-a'),
                         ('mapo', '-p'),
                         ('reduceOutputPercent', '-u'),
                         ('fin', '-f'),
                         ('mapreduce.input.fileinputformat.split.min', '-b'),
                         ('mapreduce.input.fileinputformat.split.max', '-d'),
                         ('dfs', '-g'),
                         ('mapreduce.map.sort.spill.percent', '-h'),
                         ('yarn.nodemanager.resource.memory-mb', '-j'),
                         ('mapreduce.task.io.sort.mb', '-k'),
                         ('mapreduce.task.io.sort.factor', '-n'),
                         ('mapreduce.reduce.shuffle.merge.percent', '-o'),
                         ('mapreduce.reduce.shuffle.input.buffer.percent', '-z'),
                         ('mapreduce.job.reduce.slowstart.completedmaps', '-q'),
                         ('record', '--recordSize'),
                         ('mapreduceMapMemory', '--mapreduceMapMemory'),
                         ('mapreduceReduceMemory', '--mapreduceReduceMemory'),
                         ('amResourceMB', '--amResourceMB'),
                         ('mapCpuVcores', '--mapCpuVcores'),
                         ('reduceCpuVcores', '--reduceCpuVcores'),
                         ('mapIntensity', '--mapIntensity'),
                         ('mapSortIntensity', '--mapSortIntensity'),
                         ('reduceIntensity', '--reduceIntensity'),
                         ('reduceSortIntensity', '--reduceSortIntensity'),
                         ('combinerIntensity', '--combinerIntensity'),
                         ('queueIDs', '--queueIDs'),
                         ('schQueues', '--schQueues')]
        self.options_startswith_dict = {key: value for key, value in self.options_startswith_list}

        # option names for presentation
        self.optionsnamelist = [('-m', 'Memory: '),
                         ('-c', 'Core Count: '),
                         ('-s', 'Core Speed: '),
                         ('-v', 'Max Read Speed: '),
                         ('-w', 'Max Write Speed: '),
                         ('-x', 'Min Read Speed: '),
                         ('-y', 'Min Write Speed: '),
                         ('-l', 'Link Speed: '),
                         ('-r', 'Reducer Count: '),
                         ('-a', 'Application Size: '),
                         ('-p', 'Map Output Percent: '),
                         ('-u', 'Reduce Output Percent: '),
                         ('-f', 'Final Output Percent: '),
                         ('-b', 'mapreduce.input.fileinputformat.split.minsize: '),
                         ('-d', 'mapreduce.input.fileinputformat.split.maxsize: '),
                         ('-g', 'dfsBlocksize: '),
                         ('-h', 'mapreduce.map.sort.spill.percent: '),
                         ('-j', 'yarn.nodemanager.resource.memory-mb: '),
                         ('-k', 'mapreduce.task.io.sort.mb: '),
                         ('-n', 'mapreduce.task.io.sort.factor: '),
                         ('-o', 'mapreduce.reduce.shuffle.merge.percent: '),
                         ('-z', 'mapreduce.reduce.shuffle.input.buffer.percent: '),
                         ('-q', 'mapreduce.job.reduce.slowstart.completedmaps: '),
                         ('--recordSize', 'record size: '),
                         ('--mapreduceMapMemory', 'mapreduce.map.memory.mb: '),
                         ('--mapreduceReduceMemory', 'mapreduce.reduce.memory.mb: '),
                         ('--amResourceMB', 'yarn.app.mapreduce.am.resource.mb: '),
                         ('--mapCpuVcores', 'mapreduce.map.cpu.vcores: '),
                         ('--reduceCpuVcores', 'mapreduce.reduce.cpu.vcores: '),
                         ('--mapIntensity', 'Map Intensity: '),
                         ('--mapSortIntensity', 'Map Sort Intensity: '),
                         ('--reduceIntensity', 'Reduce Intensity: '),
                         ('--reduceSortIntensity', 'Reduce Sort Intensity: '),
                         ('--combinerIntensity', 'Combiner Intensity: ')]
        self.optionsnamedict = {key: value for key, value in self.optionsnamelist}

        # read file to count applications
        TROIKAInput = open(self.args.TROIKAInputFileName,"r")
        self.appCount = 0
        for line in TROIKAInput:
            if line[0] == 'a':
                self.appCount += 1
        
    @property
    def systemsToBeTested(self):
        return self._systemsToBeTested

    @property
    def useRSM(self):
        return self.useRSM
    
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
                    TROIKAInput.close()
                    return self.DEFAULT_RESOURCEMEMSIZE

    def getPhysicalMem(self):
        # read from filename
        TROIKAInput = open(self.args.TROIKAInputFileName,"r")

        for line in TROIKAInput:
            if line[0] == 'm':
                TROIKAInput.close()
                return long(line.split()[5])/1048576

    # returns number of nodes and links in the cluster
    def clusterResourceCounter(self):

        numberOfNodes = 0
        numberOfLinks = 0

        # Count the number of nodes and the number of links while calulating the overall cost
        # read from filename
        TROIKAInput = open(self.args.TROIKAInputFileName,"r")

        for line in TROIKAInput:
            if line[0] == 'n':
                numberOfNodes += 1
            elif line[0] == 'l':
                numberOfLinks += 1
        TROIKAInput.close()

        return numberOfNodes, numberOfLinks

    # returns the cost of cluster based on the cost of a single node and a link
    def clusterCost(self, nodeCost, linkCost):
        numberOfNodes, numberOfLinks = self.clusterResourceCounter()
        totalCost = (numberOfNodes* nodeCost) + (numberOfLinks* linkCost)
        return totalCost

    def isNumber(self, word):
        try:
            float(word)
            return True
        except ValueError:
            return False

    def setNumberOfLists(self):
        numberOfRes = 0
        lisOfWords = self._systemsToBeTested[0].split()

        queueIDs_schQueues_Flag = False
        checkNext = False

        for word in lisOfWords:
            if word == 'res':
                checkNext = True
            elif checkNext:
                checkNext = False
                if word == 'queueIDs' or word == 'schQueues':
                    queueIDs_schQueues_Flag = True
                else:
                    numberOfRes += 1
            elif queueIDs_schQueues_Flag:
                if self.isNumber(word):
                    numberOfRes += 1
                else:
                    queueIDs_schQueues_Flag = False

        self.numberOfResources = numberOfRes

    # returns the number of non-range res
    def getNumberOfLists(self):
        resLocations = []
        lisOfWords = self._systemsToBeTested[0].split()

        queueIDs_schQueues_Flag = False
        checkNext = False
        index = 0

        for word in lisOfWords:
            if word == 'res':
                checkNext = True
            elif checkNext:
                checkNext = False
                if word == 'queueIDs' or word == 'schQueues':
                    queueIDs_schQueues_Flag = True
                else:
                    resLocations.append(long(index +1))
            elif queueIDs_schQueues_Flag:
                if self.isNumber(word):
                    resLocations.append(long(index))
                else:
                    queueIDs_schQueues_Flag = False

            index += 1

        return resLocations

    def createSortedLists(self):
        resLocations = self.getNumberOfLists()
        sortedSystems = [[] for i in range(self.numberOfResources)]

        for i in xrange(self.numberOfResources):
            sortedSystems[i].extend(sorted(self._systemsToBeTested, key=lambda mem: float(mem.split(None, resLocations[i]+1)[resLocations[i]]), reverse=True))

        return sortedSystems

    def checkSampleInsufficiency(self):

        if self.args.loglevel > 2:
            print "[LOG]: self.numberOfConfig: " +  str(self.numberOfBundles)
            print "[LOG]: self.numberOfResources: " + str(self.numberOfResources)
            print "[LOG]: self.SAMPLES_PER_PARAM: " + str(self.SAMPLES_PER_PARAM)

        # first order RSM model
        if self.RSM_MODEL == 1 and (self.numberOfBundles < self.numberOfResources*self.SAMPLES_PER_PARAM):
            print "Number of Alternatives: " + str(self.numberOfBundles) + " required Samples: " + str(self.numberOfResources*self.SAMPLES_PER_PARAM)
            return True     # Insufficient number of configs!

        # second order RSM model
        elif self.RSM_MODEL == 2 and (self.numberOfBundles < ((self.numberOfResources*(self.numberOfResources+3))/2)*self.SAMPLES_PER_PARAM):
            print "Number of Alternatives: " + str(self.numberOfBundles) + " required Samples: " + str(((self.numberOfResources*(self.numberOfResources+3))/2)*self.SAMPLES_PER_PARAM)
            return True     # Insufficient number of configs!

        return False

    # eliminate systems with insufficient memory and the ones having higher cost than the budget
    # returns number of configs in the new list
    def eliminate(self):
        # insufficient memory and budget satisfiability check
        requiredMemory = self.getNodemanagerResourceMemory()
        physicalMem = self.getPhysicalMem()

        for x in self.content:
            # a bundle  definition is found
            if x.startswith('bundle'):
                
                lisOfWords = x.split()
                memIndex = -1
                nmResourceMemIndex = -1
                nodeCost = 0
                linkCost = 0

                for word in lisOfWords:
                    # find memory (if provided)
                    if word.startswith('me'):
                        memIndex=lisOfWords.index(word) + 1
                        physicalMem = long(lisOfWords[memIndex])
                    # find nodeCost (if provided)
                    elif word.startswith('nodec'):
                        nodeCost = long(lisOfWords[lisOfWords.index(word) + 1])
                    # find linkCost (if provided)
                    elif word.startswith('linkc'):
                        linkCost = long(lisOfWords[lisOfWords.index(word) + 1])
                    # find yarn.nodemanager.resource.memory-mb (if provided)
                    elif word.startswith('yarn.nodemanager.resource.memory-mb'):
                        nmResourceMemIndex = lisOfWords.index(word) + 1
                        requiredMemory = long(lisOfWords[nmResourceMemIndex])
                    
                # get total cost of the cluster
                totalCost = self.clusterCost(nodeCost, linkCost)

                # check if memory is sufficient
                # and check if the overall cost is less than the budget
                if (physicalMem >= requiredMemory) and (totalCost <= self.args.budget):
                    self._systemsToBeTested.append(x)
                    self.simID_clusterCost[lisOfWords[1]] = totalCost
      
        self.numberOfBundles = len(self._systemsToBeTested)

        # sanity check for existence of at least one available bundle
        if self.numberOfBundles == 0:
            sys.exit('Please check your input list and make sure that for at least one of the systems in it, you have sufficient memory and your budget is above the cost of the cluster')

        # set self.numberOfResources
        self.setNumberOfLists()

        # sanity check for RSM compliance (if either RSM was not intended to be used or if there is 
        # insufficient number of available bundles, then use multi-dimensional optimization)
        if self.useRSM and self.checkSampleInsufficiency():
            print "Sample size is less than user's request after the pruning. RSM cannot be used. Fallback to multidimensional optimization."
            self.useRSM = False

        if not self.useRSM:
            # create sorted list for each non-range res
            self.sortedSystems = self.createSortedLists()

        return self.numberOfBundles

    def extractCommand(self, system):

        simId = system.split(None, 2)[1]
        rangedCommand = []
        regularCommand = ""
        totSimCount = self.DEFAULT_SIMCOUNT
        words = system.split()

        # signal the existence of the commands with the list
        queueIDs_schQueues_Flag = False
        rangeresCount = -1
        resCount = -1

        for word in words:

            if word == 'rangeres':
                rangeresCount = 5
                queueIDs_schQueues_Flag = False
            elif word == "res":
                resCount = 2
                queueIDs_schQueues_Flag = False
            elif word.startswith('repeat'):
                totSimCount = long(words[words.index(word)+1])
                queueIDs_schQueues_Flag = False

            elif word.startswith('nodec') or word.startswith('linkc'):
                resCount = -1

            # detect range resource names and values (bottom limit upper limit and step size)
            elif (rangeresCount > 0) and (rangeresCount <= 4):

                if rangeresCount == 4:
                    if word.startswith('red'):
                        rangedCommand.append(" -r ")
                    # more rangeres commands can be added here...
                else:
                    rangedCommand.append(word)

            # detect resource names
            elif resCount == 1:

                if word in self.options_equal_dict:
                    regularCommand += ' ' + self.options_equal_dict[word] + ' '

                else:
                    for k,v in self.options_startswith_dict.iteritems():
                        if k.startswith(word):
                            regularCommand +=  ' ' + v + ' '

                            if v == '--schQueues' or v == '--queueIDs':
                                queueIDs_schQueues_Flag = True

            # detect resource values 
            elif resCount == 0:
                regularCommand += str(word)
                if queueIDs_schQueues_Flag:
                    regularCommand += " "
                    continue

            # decrease resource counters
            rangeresCount -= 1
            resCount -= 1

        return regularCommand, simId, rangedCommand, totSimCount

    def getFromCache(self, simId):
        if simId in self.resultCache:
            return self.resultCache[simId].split('$')[0], self.resultCache[simId].split('$')[1]
        return None, None

    # perform the simulation with the given parameters using the TROIKA simulator
    # return the performance test result
    def performanceTest(self, command, simId, rangedCommand, totSimCount):

        bestToBeWritten, bestCommand = self.getFromCache(simId)
        
        if bestToBeWritten is not None:
            if self.args.loglevel > 1:
                print "[LOG]: The result is in the cache"
            return bestToBeWritten, bestCommand

        if self.args.loglevel > 1:
            print "[LOG]: The result is NOT in the cache"

        rangedCommandCount = len(rangedCommand) / 4
        bestElapsedTime = sys.maxint

        # for each ranged command...
        for rngCmd in xrange(rangedCommandCount):

            command += rangedCommand[rngCmd*4]

            currentRangeresCount = int(rangedCommand[rngCmd*4+1])

            while currentRangeresCount <= int(rangedCommand[rngCmd*4+2]):
                for sim in xrange(totSimCount):

                    if self.args.loglevel > 1:
                        print self.args.executablePath + command + str(currentRangeresCount)

                    os.system(self.args.executablePath + command + str(currentRangeresCount))
                    if self.args.loglevel > 0:
                        print "[LOG]: Progress: " +  str(sim+1) + "/" + str(totSimCount) +" of the simulation of system with ID: " + str(simId) + " is finished. Red count: " + str(currentRangeresCount)

                time.sleep(1)
                # note: this must be reducer...
                with open("test" + str(currentRangeresCount) + ".txt") as f:
                    content = f.readlines()

                os.system("rm test" + str(currentRangeresCount) + ".txt")

                ElapsedTotal = 0
                AvgMap = 0
                AvgReduce = 0
                AvgShuffle = 0
                TotalMap = 0

                for i in xrange(totSimCount):
                    ElapsedTotal += float(content[0 +5*(self.appCount-1+i)])
                    AvgMap += float(content[1 +5*(self.appCount-1+i)])
                    AvgReduce += float(content[2 +5*(self.appCount-1+i)])
                    AvgShuffle += float(content[3 +5*(self.appCount-1+i)])
                    TotalMap += float(content[4 +5*(self.appCount-1+i)])

                toBeWritten = str(simId) + " " +str(ElapsedTotal/totSimCount) + " " + str(AvgMap/totSimCount) + " " + str(AvgReduce/totSimCount) + " " + str(AvgShuffle/totSimCount) + " " + str(TotalMap/totSimCount) + "\n"

                if (ElapsedTotal/totSimCount) < bestElapsedTime:
                    bestElapsedTime = (ElapsedTotal/totSimCount)
                    bestToBeWritten = toBeWritten
                    bestCommand = "" + command + str(currentRangeresCount)

                # increase the rangeres count for the next phase
                currentRangeresCount += int(rangedCommand[rngCmd*4+3])

        return bestToBeWritten, bestCommand

    # update the bestperformance and bestcommand (for those who yield fastest)
    # return True if the new performance is better
    # return False o/w
    def isNewPerformanceBetter(self, toBeWritten, bestCommand):

        # save the result in cache of results for faster access (if not already in the cache)
        # hash table to be used as a cache for already completed computations.
        # (key = simID, value = performance result + corresponding configuration)
        if not (toBeWritten.split(None, 1)[0] in self.resultCache):
            self.resultCache[toBeWritten.split(None, 1)[0]] = toBeWritten + " $" + bestCommand

            if self.args.loglevel > 1:
                print "[LOG]: Added to cache " + str(self.resultCache[toBeWritten.split(None, 1)[0]])
        else:
            return False

        # check whether the new result is better than the best
        # if it is better than the best, update the best result and corresponding config
        if float(toBeWritten.split(None, 2)[1]) < float(self.bestPerformance.split(None, 2)[1]):
            self.bestPerformance = toBeWritten
            self.bestCommand = bestCommand
            return True
        return False

    def outofToleranceLimit(self, toBeWritten):

        if self.args.loglevel > 1:
            print "[LOG]: Tolerance: " + str(self.args.testTolerance)

        if float(toBeWritten.split(None, 2)[1]) > float(self.bestPerformance.split(None, 2)[1]) * (1+self.args.testTolerance):
            return True
        return False

    def getMaxMinResList(self, order):

        maxList = [0.0] * self.numberOfResources
        minList = [sys.float_info.max] * self.numberOfResources
        rowID = 0
        colID = 0
        numberOfOnes = -1

        if order == 1:
            numberOfOnes = self.numberOfResources*self.SAMPLES_PER_PARAM

        elif order == 2:
            numberOfOnes = ((self.numberOfResources*(self.numberOfResources+3))/2)*self.SAMPLES_PER_PARAM

        resMatrix = asmatrix(ones((self.numberOfBundles,self.numberOfResources)))
        resDoneFlag = False

        for system in self._systemsToBeTested:
            lisOfWords = system.split()
            resIndex = 0
            queueIDs_schQueues_Flag = False
            checkNext = False
            index = 0

            for word in lisOfWords:
                if word == 'res':
                    checkNext = True
                elif checkNext:
                    checkNext = False
                    if word == 'queueIDs' or word == 'schQueues':
                        queueIDs_schQueues_Flag = True
                    else:
                        resMatrix[rowID, colID] = float(lisOfWords[long(index +1)])
                        colID += 1

                        if not resDoneFlag:
                            if maxList[resIndex] < resMatrix[rowID, colID-1]:
                                maxList[resIndex] = resMatrix[rowID, colID-1]
                            if minList[resIndex] > resMatrix[rowID, colID-1]:
                                minList[resIndex] = resMatrix[rowID, colID-1]
                            resIndex += 1

                elif queueIDs_schQueues_Flag:
                    if self.isNumber(word):

                        resMatrix[rowID, colID] = float(lisOfWords[long(index)])
                        colID += 1

                        if not resDoneFlag:
                            if maxList[resIndex] < resMatrix[rowID, colID-1]:
                                maxList[resIndex] = resMatrix[rowID, colID-1]
                            if minList[resIndex] > resMatrix[rowID, colID-1]:
                                minList[resIndex] = resMatrix[rowID, colID-1]
                            resIndex += 1
                    else:
                        queueIDs_schQueues_Flag = False

                index += 1

            if rowID == numberOfOnes -1:
                resDoneFlag = True

            rowID += 1
            colID = 0

        for index in xrange(len(minList)):
            if maxList[index] == minList[index] :
                print "[LOG]: Parameter value " + str(maxList[index]) + " is the same for the specific parameter"
                sys.exit('Make sure that samples contain at least one different value for each parameter!')

        return maxList, minList, resMatrix

    # returns the X matrix upon calculating coding scheme
    def calculateCodingScheme(self, order):

        maxRes, minRes, resMatrix = self.getMaxMinResList(order)
        numberOfRows = -1
        numberOfCols = -1

        if order == 1:
            numberOfRows = self.numberOfResources*self.SAMPLES_PER_PARAM
            numberOfCols = self.numberOfResources+1

        elif order == 2:
            numberOfRows = ((self.numberOfResources*(self.numberOfResources+3))/2)*self.SAMPLES_PER_PARAM
            numberOfCols = ((self.numberOfResources*(self.numberOfResources+3))/2)+1

        # create X matrix
        X_matrix = asmatrix(ones((numberOfRows,numberOfCols)))

        # creaete X_matrix_nat
        X_matrix_nat = asmatrix(ones((numberOfRows,numberOfCols)))

        # create X_matrix_complete
        X_matrix_complete = asmatrix(ones((self.numberOfBundles,numberOfCols)))

        for rID in xrange(numberOfRows):
            for cID in xrange(1, self.numberOfResources+1):
                X_matrix[rID, cID] = (resMatrix[rID, cID-1]-((maxRes[cID-1]+minRes[cID-1])/2.0))/((maxRes[cID-1]-minRes[cID-1])/2.0)
                X_matrix_nat[rID, cID] = resMatrix[rID, cID-1]
                X_matrix_complete[rID, cID] = (resMatrix[rID, cID-1]-((maxRes[cID-1]+minRes[cID-1])/2.0))/((maxRes[cID-1]-minRes[cID-1])/2.0)
        
        for rID in xrange(numberOfRows, self.numberOfBundles):
            for cID in xrange(1, self.numberOfResources+1):
                X_matrix_complete[rID, cID] = (resMatrix[rID, cID-1]-((maxRes[cID-1]+minRes[cID-1])/2.0))/((maxRes[cID-1]-minRes[cID-1])/2.0)

        # set the remaining parts of the X_matrix for 2nd order RSM model with interaction
        if order == 2:
            # squares in the formula
            for rID in xrange(numberOfRows):
                for cID in xrange(self.numberOfResources+1, 2*self.numberOfResources+1):
                    X_matrix[rID, cID] = X_matrix[rID, cID-self.numberOfResources]*X_matrix[rID, cID-self.numberOfResources]
                    X_matrix_nat[rID, cID] = X_matrix_nat[rID, cID-self.numberOfResources]*X_matrix_nat[rID, cID-self.numberOfResources]
                    X_matrix_complete[rID, cID] = X_matrix_complete[rID, cID-self.numberOfResources]*X_matrix_complete[rID, cID-self.numberOfResources]

            # squares in the formula
            for rID in xrange(numberOfRows, self.numberOfBundles):
                for cID in xrange(self.numberOfResources+1, 2*self.numberOfResources+1):
                    X_matrix_complete[rID, cID] = X_matrix_complete[rID, cID-self.numberOfResources]*X_matrix_complete[rID, cID-self.numberOfResources]

            # multiplications among different terms
            for rID in xrange(numberOfRows):
                # the first term is interacting with k-1 terms
                interactingTermCount = self.numberOfResources-1
                head = self.numberOfResources-interactingTermCount
                interactingOrder = head+1

                for cID in xrange(2*self.numberOfResources+1, numberOfCols):
                    # calculate the value
                    X_matrix[rID, cID] = X_matrix[rID, head]*X_matrix[rID, interactingOrder]
                    X_matrix_complete[rID, cID] = X_matrix_complete[rID, head]*X_matrix_complete[rID, interactingOrder]
                    X_matrix_nat[rID, cID] = X_matrix_nat[rID, head]*X_matrix_nat[rID, interactingOrder]

                    if interactingOrder == self.numberOfResources:
                        interactingTermCount-=1
                        head = self.numberOfResources-interactingTermCount
                        interactingOrder = head+1
                    else:
                        interactingOrder += 1

            # multiplications among different terms
            for rID in xrange(numberOfRows, self.numberOfBundles):
                # the first term is interacting with k-1 terms
                interactingTermCount = self.numberOfResources-1
                head = self.numberOfResources-interactingTermCount
                interactingOrder = head+1

                for cID in xrange(2*self.numberOfResources+1, numberOfCols):
                    # calculate the value
                    X_matrix_complete[rID, cID] = X_matrix_complete[rID, head]*X_matrix_complete[rID, interactingOrder]

                    if interactingOrder == self.numberOfResources:
                        interactingTermCount-=1
                        head = self.numberOfResources-interactingTermCount
                        interactingOrder = head+1
                    else:
                        interactingOrder += 1

        return maxRes, minRes, X_matrix, X_matrix_nat, X_matrix_complete

    def generateYmatrix(self, numberOfRows):

        # this matrix will keep the sample results gathered from simulation
        y_matrix = asmatrix(ones((numberOfRows,1)))

        for sysID in xrange(numberOfRows):
            if self.args.loglevel > 0:
                print("[LOG]: Start simulation for simID: " + str(self._systemsToBeTested[sysID].split(None, 2)[1]))

            regularCommand, simId, rangedCommand, totSimCount = self.extractCommand(self._systemsToBeTested[sysID])
            if self.args.loglevel > 1:
                print "[LOG]: regCommand " + str(regularCommand) + " simID " + str(simId) + " totSimCount " + str(totSimCount)
                print "[LOG]: Ranged " + str(rangedCommand)

            # perform the test 
            toBeWritten, bestCommand = self.performanceTest(regularCommand, simId, rangedCommand, totSimCount)

            print "toBeWritten " + str(toBeWritten)
            print "bestCommand " + str(bestCommand)

            y_matrix[sysID] = float(toBeWritten.split(None, 2)[1])

            if self.args.loglevel > 0:
                print("[LOG]: Finish simulation for simID: " + str(self._systemsToBeTested[sysID].split(None, 2)[1]))

            # update the bestperformance and bestcommand (for those who yield fastest)
            self.isNewPerformanceBetter(toBeWritten, bestCommand)

        return y_matrix

    def getXprime_X(self, X_matrix):
        return X_matrix.T*X_matrix

    def getXprime_y(self, X_matrix, y_matrix):
        return X_matrix.T*y_matrix

    def getb_matrix(self, Xprime_X, Xprime_y):
        return Xprime_X.I*Xprime_y

    def getRowColID(self, coef, k):

        posInTriangle = coef - 2*k
        coefRowID = k-1
        coefColID = 0
        value = 1
        addMe = k-1
        for i in xrange(k-1):
            value += addMe
            if posInTriangle < value:
                value -= addMe
                coefRowID = i+1
                break
            addMe -=1
        coefColID = posInTriangle-value+1
        return coefRowID, coefColID

    def get_sysResMatrix(self):

        numCols = 0

        if self.RSM_MODEL == 1:
            numCols = self.numberOfResources +1
        else:
            numCols = ((self.numberOfResources*(self.numberOfResources+3))/2) +1

        sysResMatrix = asmatrix(ones((self.numberOfBundles,numCols)))

        rowID = 0
        colID = 0

        for system in self._systemsToBeTested:
            lisOfWords = system.split()
            queueIDs_schQueues_Flag = False
            checkNext = False
            index = 0

            for word in lisOfWords:
                if word == 'res':
                    checkNext = True
                elif checkNext:
                    checkNext = False
                    if word == 'queueIDs' or word == 'schQueues':
                        queueIDs_schQueues_Flag = True
                    else:
                        sysResMatrix[rowID, colID+1] = float(lisOfWords[long(index +1)])
                        colID += 1
                elif queueIDs_schQueues_Flag:
                    if self.isNumber(word):
                        sysResMatrix[rowID, colID+1] = float(lisOfWords[long(index)])
                        colID += 1
                    else:
                        queueIDs_schQueues_Flag = False

                index += 1

            rowID += 1
            colID = 0

        # if second order then calculate the remaining parts
        if self.RSM_MODEL == 2:

            # squares in the formula
            for rID in xrange(self.numberOfBundles):
                for cID in xrange(self.numberOfResources+1, 2*self.numberOfResources+1):
                    sysResMatrix[rID, cID] = sysResMatrix[rID, cID-self.numberOfResources]*sysResMatrix[rID, cID-self.numberOfResources]

            # multiplications among different terms
            for rID in xrange(self.numberOfBundles):
                # the first term is interacting with k-1 terms
                interactingTermCount = self.numberOfResources-1
                head = self.numberOfResources-interactingTermCount
                interactingOrder = head+1

                for cID in xrange(2*self.numberOfResources+1, numCols):
                    # calculate the value
                    sysResMatrix[rID, cID] = sysResMatrix[rID, head]*sysResMatrix[rID, interactingOrder]

                    if interactingOrder == self.numberOfResources:
                        interactingTermCount-=1
                        head = self.numberOfResources-interactingTermCount
                        interactingOrder = head+1
                    else:
                        interactingOrder += 1

        return sysResMatrix

    def get_y_head_matrix(self, b_matrix, X_matrix_complete, b_matrix_nat=None):
        # calculate and store y_head value for all the systems in input file, containing the bundles
        
        if b_matrix_nat is not None:
            sysResMatrix = self.get_sysResMatrix()

            print "ST----------PLAIN--NEW------------"
            print sysResMatrix * b_matrix_nat
            print "EN----------PLAIN--NEW------------"

        return (X_matrix_complete * b_matrix)

    def getSortedSystems(self, y_head_list):
        return [a for (b,a) in sorted(zip(y_head_list,self._systemsToBeTested))]

    def tester(self):
        optimizerIsSelected = False

        while not optimizerIsSelected:
            if not self.useRSM:

                optimizerIsSelected = True

                for listID in xrange(self.numberOfResources):

                    for configID in xrange(self.numberOfBundles):
                        if self.args.loglevel > 0:
                            print("[LOG]: Start simulation for simID: " + str(self.sortedSystems[listID][configID].split(None, 2)[1]))

                        regularCommand, simId, rangedCommand, totSimCount = self.extractCommand(self.sortedSystems[listID][configID])

                        print "totSimCount " + str(totSimCount)

                        if self.args.loglevel > 1:
                            print "[LOG]: regCommand " + str(regularCommand) + " simID " + str(simId) + " totSimCount " + str(totSimCount)
                            print "[LOG]: Ranged " + str(rangedCommand)

                        # perform the test 
                        toBeWritten, bestCommand = self.performanceTest(regularCommand, simId, rangedCommand, totSimCount)
                        if self.args.loglevel > 0:
                            print("[LOG]: Finish simulation for simID: " + str(self.sortedSystems[listID][configID].split(None, 2)[1]))

                        # check if the result is better than the previous test
                        if (not self.isNewPerformanceBetter(toBeWritten, bestCommand)) and self.outofToleranceLimit(toBeWritten):
                            if self.args.loglevel > 1:
                                print "[LOG]: Intolerable..."
                            break
            else:
                # calculate coding scheme to map all the variable values between [-1, 1] and generate X_matrix
                maxRes, minRes, X_matrix, X_matrix_nat, X_matrix_complete = self.calculateCodingScheme(self.RSM_MODEL)

                if self.args.loglevel > 2:
                    print "[LOG]: maxRes " + str(maxRes)
                    print "-------------------------"
                    print "[LOG]: minRes " + str(minRes)
                    print "-------------------------"
                    print "[LOG]: X_matrix " + str(X_matrix)
                    print "-------------------------"

                # calculate X'X
                Xprime_X = self.getXprime_X(X_matrix)

                if self.args.debugMode == 1:
                    #calculate natural X'X
                    Xprime_X_nat = self.getXprime_X(X_matrix_nat)

                # singularity check
                if linalg.det(Xprime_X) == 0:
                    self.useRSM = False
                    
                    if self.args.loglevel > 2:
                        print "[LOG]: Xprime_X " + str(Xprime_X)

                    # create sorted list for each non-range res
                    self.sortedSystems = self.createSortedLists()
                    print "Matrix is singular. RSM cannot be used. Fallback to multidimensional optimization."
                else:
                    optimizerIsSelected = True
                    # generate y matrix
                    y_matrix = self.generateYmatrix(len(X_matrix))
                    
                    # calculate X'y
                    Xprime_y = self.getXprime_y(X_matrix, y_matrix)

                    if self.args.debugMode == 1:
                        # calculate natural X'y
                        Xprime_y_nat = self.getXprime_y(X_matrix_nat, y_matrix)

                    # calculate b
                    b_matrix = self.getb_matrix(Xprime_X, Xprime_y)

                    if self.args.debugMode == 1:
                        # calculate b_matrix_nat
                        b_matrix_nat = self.getb_matrix(Xprime_X_nat, Xprime_y_nat)

                    if self.args.loglevel > 2:
                        print "START: b_matrix"
                        print len(b_matrix)
                        print b_matrix
                        print "END: b_matrix"

                        if self.args.debugMode == 1:
                            print "START: b_matrix_nat"
                            print len(b_matrix_nat)
                            print b_matrix_nat
                            print "END: b_matrix_nat"


                    if self.args.debugMode == 1:
                        # calculate y_head
                        y_head_matrix = self.get_y_head_matrix(b_matrix, X_matrix_complete, b_matrix_nat)
                    else: 
                        # calculate y_head
                        y_head_matrix = self.get_y_head_matrix(b_matrix, X_matrix_complete)

                    # print "---------------------y_head_matrix---------------------"
                    # print y_head_matrix
                    # print "---------------------y_head_matrix---------------------"

                    # convert matrix to a list
                    y_head_list = array(y_head_matrix.T)[0].tolist()
                    # sort systems based on y head
                    sortedSys = self.getSortedSystems(y_head_list)

                    for configID in xrange(self.FINAL_TEST_COUNT):
                        if self.args.loglevel > 0:
                            print("[LOG]: Start simulation for simID: " + str(sortedSys[configID].split(None, 2)[1]))

                        regularCommand, simId, rangedCommand, totSimCount = self.extractCommand(sortedSys[configID])
                        if self.args.loglevel > 1:
                            print "[LOG]: regCommand " + str(regularCommand) + " simID " + str(simId) + " totSimCount " + str(totSimCount)
                            print "[LOG]: Ranged " + str(rangedCommand)

                        # perform the test 
                        toBeWritten, bestCommand = self.performanceTest(regularCommand, simId, rangedCommand, totSimCount)
                        
                        if self.args.loglevel > 0:
                            print("[LOG]: Finish simulation for simID: " + str(sortedSys[configID].split(None, 2)[1]))
                       
                        # update the bestperformance and bestcommand (for those who yield fastest)
                        self.isNewPerformanceBetter(toBeWritten, bestCommand)

        return self.bestPerformance, self.bestCommand

    def presentResult(self):
        # print the result of the best (minimum time) simulation
        print "----------------------------------\nMinimum average elapsed time:"
        print "Configuration ID: " + str(self.bestPerformance.split(None, 1)[0])
        print "Elapsed time: " + str(self.bestPerformance.split(None, 2)[1])
        print "Average map time: " + str(self.bestPerformance.split(None, 3)[2])
        print "Average reduce time: " + str(self.bestPerformance.split(None, 4)[3])
        print "Shuffle simulation time: " + str(self.bestPerformance.split(None, 5)[4])
        print "Total map simulation time: " + str(self.bestPerformance.split(None, 6)[5])

        print "---------CONFIGURATION SUMMARY---------"
        print self.bestCommand

        listOfWords = self.bestCommand.split()

        queueIDs_params = []
        schQueues_params = []

        for word in listOfWords:

            if word in self.optionsnamedict:
                self.paramFlag_value[word] = listOfWords.index(word) + 1

            elif word == '--queueIDs':
                # get all queueID parameters
                done = False
                i = 1
                while not done:
                    # first make sure that the index exists
                    # then make sure that the element in that index contains some numbers (which means that it is a parameter)
                    if ((listOfWords.index(word) + i) < len(listOfWords)) and re.search('[0-9]', str(listOfWords[listOfWords.index(word) + i])):
                        queueIDs_params.append(str(listOfWords[listOfWords.index(word) + i]))
                        i += 1
                    else:
                        done = True

            elif word == '--schQueues':
                # get all schQueues parameters
                done = False
                i = 1
                while not done:
                    # first make sure that the index exists
                    # then make sure that the element in that index contains some numbers (which means that it is a parameter)
                    if ((listOfWords.index(word) + i) < len(listOfWords)) and re.search('[0-9]', str(listOfWords[listOfWords.index(word) + i])):
                        schQueues_params.append(str(listOfWords[listOfWords.index(word) + i]))
                        i += 1
                    else:
                        done = True

        # present recommended parameters
        for key, val in self.paramFlag_value.iteritems():
            print self.optionsnamedict[key] + listOfWords[val]

        # print contents of queue IDs and corresponding capacities
        if len(queueIDs_params) > 0:

            print "Queue IDs: ",
            for qID in queueIDs_params:
                print str(qID) + " ",

        if len(schQueues_params) > 0:

            print "\nScheduler Queue Capacities: ",
            for sCap in schQueues_params:
                print str(sCap) + " ",

        print "\nCluster Cost: " + str(self.simID_clusterCost[str(self.bestPerformance.split(None, 1)[0])])

# Use a first order RSM (y = Bo + B1X1 + B2X2 + ... + BnXn, where n = number of res)

try:
    start_time = time.time()
    myRecommender = RecommendationEngine()
    numberOfConfigs = myRecommender.eliminate()
    bestPerf, bestCmd = myRecommender.tester()

    print "Troika Recommendation Generation Time: " + str(time.time() - start_time) + " seconds"

    myRecommender.presentResult()
except IOError, e:
    print "[LOG]: Interrupted..."
    sys.exit()
