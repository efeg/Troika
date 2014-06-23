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

# python recommendationEngine.py greedyBundleOptimizer.txt -b 50000 -l 1
# greedyBundleOptimizer.txt contains a list of bundles in the following example format:
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
minw.*:\t\t\t\t\t\tminimum HD write spedd\n\
me.*:\t\t\t\t\t\tmemory size\n\
app.*:\t\t\t\t\t\tapplication size\n\
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
")

        # required argument
        self.parser.add_argument(      "configFileName",        help="Filename of the input, containing the bundles")
        # optional arguments
        self.parser.add_argument("-i", "--TROIKAInputFileName", help="Input file to be used by TROIKA",           default="input.txt")
        self.parser.add_argument("-e", "--executablePath",      help="Path to TROIKA's executable",           default="../../Debug/Troika")
        self.parser.add_argument("-b", "--budget",              help="The total cluster budget limit", type=long, default=50000)
        self.parser.add_argument("-z", "--testTolerance",       help="Tolerance percentage to worse performance", type=float)
        self.parser.add_argument("-l", "--loglevel",            help="Log level [0: none, 1: regular, 2: all]", type=int, default=1)
        self.parser.add_argument("-s", "--samplesize",          help="Samples per parameter(try to provide 1:20 ratio)", type=int, default=5)
        self.parser.add_argument("-f", "--finaltestcount",      help="Systems to be tested after fitting a model", type=int, default=3)

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
        self.DEFAULT_SIMCOUNT = 5
        self.SAMPLES_PER_PARAM = self.args.samplesize
        # how many systems will be tested after fitting a model and finding the expected best configs?
        self.FINAL_TEST_COUNT = self.args.finaltestcount

        # the best perf result and corresponding configuration result found so far
        self.bestPerformance = "0 " + str(sys.float_info.max) + " 0 0 0 0"
        self.bestCommand = ""
        self.numberOfLists = 0
        self.numberOfConfigs = 0

        # contains the systems to be tested (the list is populated in "eliminate" fn)
        self._systemsToBeTested = []
        self.sortedSystems = []
        
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

    def setNumberOfLists(self):
        numberOfRes = 0
        lisOfWords = self._systemsToBeTested[0].split()

        for word in lisOfWords:
            if word == 'res':
                numberOfRes += 1

        self.numberOfLists = numberOfRes

    # returns the number of non-range res
    def getNumberOfLists(self):
        getCounter = -1
        resLocations = []
        lisOfWords = self._systemsToBeTested[0].split()

        for word in lisOfWords:
            if word == 'res':
                # will be used to get the index of next of next word
                getCounter = 1

            elif getCounter == 0:
                resLocations.append(long(lisOfWords.index(word)) +1)

            getCounter -= 1

        return resLocations

    def createSortedLists(self):
        resLocations = self.getNumberOfLists()

        sortedSystems = [[] for i in range(self.numberOfLists)]

        for i in xrange(self.numberOfLists):
            sortedSystems[i].extend(sorted(self._systemsToBeTested, key=lambda mem: mem.split(None, resLocations[i]+1)[resLocations[i]], reverse=True))

        return sortedSystems

    # eliminate systems with insufficient memory, higher cost than the limit
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
      
        self.numberOfConfigs = len(self._systemsToBeTested)

        # sanity check for existence of at least one available bundle
        if self.numberOfConfigs == 0:
            sys.exit('Please check your input list and make sure that for at least one of the systems in it, you have sufficient memory and your budget is above the cost of the cluster')

        # set self.numberOfLists
        self.setNumberOfLists()

        # sanity check for RSM compliance (if either RSM was not intended to be used or if there is 
        # insufficient number of available bundles, then use multi-dimensional optimization)
        if self.useRSM and (self.numberOfConfigs < self.numberOfLists*self.SAMPLES_PER_PARAM):
            print "Sample size is less than user's request after the pruning. RSM cannot be used. Fallback to multidimensional optimization."
            self.useRSM = False

        if not self.useRSM:
            # create sorted list for each non-range res
            self.sortedSystems = self.createSortedLists()

        return self.numberOfConfigs

    def extractCommand(self, system):

        simId       = system.split(None, 2)[1]
        rangedCommand = []
        regularCommand = ""
        totSimCount = self.DEFAULT_SIMCOUNT
        words = system.split()

        rangeresCount = -1
        resCount = -1

        for word in words:

            if word == 'rangeres':
                rangeresCount = 5
            elif word == "res":
                resCount = 2
            elif word.startswith('repeat'):
                totSimCount = long(words[words.index(word)+1])

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

            # detect resource values 
            elif resCount == 0:
                regularCommand += str(word)

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
                    ElapsedTotal += float(content[0 +5*i])
                    AvgMap += float(content[1 +5*i])
                    AvgReduce += float(content[2 +5*i])
                    AvgShuffle += float(content[3 +5*i])
                    TotalMap += float(content[4 +5*i])

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
                print "Added to cache " + str(self.resultCache[toBeWritten.split(None, 1)[0]])
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

        print self.args.testTolerance

        if float(toBeWritten.split(None, 2)[1]) > float(self.bestPerformance.split(None, 2)[1]) * (1+self.args.testTolerance):
            return True
        return False

    def getMaxMinResList(self):
        maxList = [0.0] * self.numberOfLists
        minList = [sys.float_info.max] * self.numberOfLists
        rowID = 0
        colID = 0

        resMatrix = asmatrix(ones((self.numberOfLists*self.SAMPLES_PER_PARAM,self.numberOfLists)))

        for system in self._systemsToBeTested:
            lisOfWords = system.split()
            resIndex = 0
            getCounter = -1

            for word in lisOfWords:
                if word == 'res':
                    # will be used to get the index of next of next word
                    getCounter = 2

                elif getCounter == 0:
                    resMatrix[rowID, colID] = float(word)
                    colID += 1
                    
                    if maxList[resIndex] < float(word):
                        maxList[resIndex] = float(word)
                    if minList[resIndex] > float(word):
                        minList[resIndex] = float(word)
                    resIndex += 1
                getCounter -= 1

            if rowID == self.numberOfLists*self.SAMPLES_PER_PARAM -1:
                break

            rowID += 1
            colID = 0

        for index in xrange(len(minList)):
            if maxList[index] == minList[index] :
                print "Parameter value " + str(maxList[index]) + " is the same for the specific parameter"
                sys.exit('Make sure that samples contain at least one different value for each parameter!')

        return maxList, minList, resMatrix

    # returns the X matrix upon calculating coding scheme
    def calculateCodingScheme(self):

        maxRes, minRes, resMatrix = self.getMaxMinResList()

        # create x matrix
        # number of rows = self.numberOfLists*self.SAMPLES_PER_PARAM
        # number of columns = number of res + 1 = self.numberOfLists + 1
        X_matrix = asmatrix(ones((self.numberOfLists*self.SAMPLES_PER_PARAM,self.numberOfLists +1)))

        for rID in xrange(self.numberOfLists*self.SAMPLES_PER_PARAM):
            for cID in xrange(1, self.numberOfLists + 1):
                X_matrix[rID, cID] = (resMatrix[rID, cID-1]-((maxRes[cID-1]+minRes[cID-1])/2.0))/((maxRes[cID-1]-minRes[cID-1])/2.0)
        return maxRes, minRes, X_matrix

    def generateYmatrix(self):

        # this matrix will keep the sample results gathered from simulation
        y_matrix = asmatrix(ones((self.numberOfLists*self.SAMPLES_PER_PARAM,1)))

        for sysID in xrange(self.numberOfLists*self.SAMPLES_PER_PARAM):
            if self.args.loglevel > 0:
                print("[LOG]: Start simulation for simID: " + str(self._systemsToBeTested[sysID].split(None, 2)[1]))

            regularCommand, simId, rangedCommand, totSimCount = self.extractCommand(self._systemsToBeTested[sysID])
            if self.args.loglevel > 1:
                print "regCommand " + str(regularCommand) + " simID " + str(simId) + " totSimCount " + str(totSimCount)
                print "Ranged " + str(rangedCommand)

            # perform the test 
            toBeWritten, bestCommand = self.performanceTest(regularCommand, simId, rangedCommand, totSimCount)

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

    def get_decoded_b_matrix(self, b_matrix, maxRes, minRes):

        decoded_b_matrix = asmatrix(ones((len(b_matrix),1)))

        decoded_b_matrix[0,0] = b_matrix[0,0] 

        for row in xrange(1, len(b_matrix)):
            decoded_b_matrix[0,0] -= ((maxRes[row-1]+minRes[row-1])/(maxRes[row-1]-minRes[row-1]))*b_matrix[row,0]
            decoded_b_matrix[row,0] = b_matrix[row,0] / ((maxRes[row-1]-minRes[row-1])/2.0)

        return decoded_b_matrix

    def get_sysResMatrix(self):
        sysResMatrix = asmatrix(ones((self.numberOfConfigs,self.numberOfLists +1)))

        rowID = 0
        colID = 0

        for system in self._systemsToBeTested:
            lisOfWords = system.split()
            getCounter = -1

            for word in lisOfWords:
                if word == 'res':
                    # will be used to get the index of next of next word
                    getCounter = 2

                elif getCounter == 0:
                    sysResMatrix[rowID, colID+1] = float(word)
                    colID += 1
                getCounter -= 1

            rowID += 1
            colID = 0

        return sysResMatrix

    def get_y_head_matrix(self, decoded_b_matrix):
        # calculate and store y_head value for all the systems in input file, containing the bundles
        sysResMatrix = self.get_sysResMatrix()
        return sysResMatrix * decoded_b_matrix

    def getSortedSystems(self, y_head_list):
        return [a for (b,a) in sorted(zip(y_head_list,self._systemsToBeTested))]

    def tester(self):

        if not self.useRSM:
            for listID in xrange(self.numberOfLists):

                for configID in xrange(self.numberOfConfigs):
                    if self.args.loglevel > 0:
                        print("[LOG]: Start simulation for simID: " + str(self.sortedSystems[listID][configID].split(None, 2)[1]))

                    regularCommand, simId, rangedCommand, totSimCount = self.extractCommand(self.sortedSystems[listID][configID])
                    if self.args.loglevel > 1:
                        print "regCommand " + str(regularCommand) + " simID " + str(simId) + " totSimCount " + str(totSimCount)
                        print "Ranged " + str(rangedCommand)

                    # perform the test 
                    toBeWritten, bestCommand = self.performanceTest(regularCommand, simId, rangedCommand, totSimCount)
                    if self.args.loglevel > 0:
                        print("[LOG]: Finish simulation for simID: " + str(self.sortedSystems[listID][configID].split(None, 2)[1]))

                    # check if the result is better than the previous test
                    if (not self.isNewPerformanceBetter(toBeWritten, bestCommand)) and self.outofToleranceLimit(toBeWritten):
                        if self.args.loglevel > 1:
                            print "Intolerable..."
                        break
        else:
            # calculate coding scheme to map all the variable values between [-1, 1] and generate X_matrix
            maxRes, minRes, X_matrix = self.calculateCodingScheme()
            # generate y matrix
            y_matrix = self.generateYmatrix()
            # calculate X'X
            Xprime_X = self.getXprime_X(X_matrix)
            # calculate X'y
            Xprime_y = self.getXprime_y(X_matrix, y_matrix)
            # calculate b
            b_matrix = self.getb_matrix(Xprime_X, Xprime_y)
            # calculate decoded b
            decoded_b_matrix = self.get_decoded_b_matrix(b_matrix, maxRes, minRes)
            # calculate y_head
            y_head_matrix = self.get_y_head_matrix(decoded_b_matrix)
            # convert matrix to a list
            y_head_list = array(y_head_matrix.T)[0].tolist()
            # sort systems based on y head
            sortedSys = self.getSortedSystems(y_head_list)

            for configID in xrange(self.FINAL_TEST_COUNT):
                if self.args.loglevel > 0:
                    print("[LOG]: Start simulation for simID: " + str(sortedSys[configID].split(None, 2)[1]))

                regularCommand, simId, rangedCommand, totSimCount = self.extractCommand(sortedSys[configID])
                if self.args.loglevel > 1:
                    print "regCommand " + str(regularCommand) + " simID " + str(simId) + " totSimCount " + str(totSimCount)
                    print "Ranged " + str(rangedCommand)

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

        results = [0] * 34
        for word in listOfWords:

            if word == '-m':
                results[0] = listOfWords.index(word) + 1
            elif word == '-c':
                results[1] = listOfWords.index(word) + 1
            elif word == '-s':
                results[2] = listOfWords.index(word) + 1
            elif word == '-v':
                results[3] = listOfWords.index(word) + 1
            elif word == '-w':
                results[4] = listOfWords.index(word) + 1
            elif word == '-x':
                results[5] = listOfWords.index(word) + 1
            elif word == '-y':
                results[6] = listOfWords.index(word) + 1
            elif word == '-l':
                results[7] = listOfWords.index(word) + 1
            elif word == '-r':
                results[8] = listOfWords.index(word) + 1
            elif word == '-a':
                results[9] = listOfWords.index(word) + 1
            elif word == '-p':
                results[10] = listOfWords.index(word) + 1
            elif word == '-u':
                results[11] = listOfWords.index(word) + 1
            elif word == '-f':
                results[12] = listOfWords.index(word) + 1
            elif word == '-b':
                results[13] = listOfWords.index(word) + 1
            elif word == '-d':
                results[14] = listOfWords.index(word) + 1
            elif word == '-g':
                results[15] = listOfWords.index(word) + 1
            elif word == '-h':
                results[16] = listOfWords.index(word) + 1
            elif word == '-j':
                results[17] = listOfWords.index(word) + 1
            elif word == '-k':
                results[18] = listOfWords.index(word) + 1
            elif word == '-n':
                results[19] = listOfWords.index(word) + 1
            elif word == '-o':
                results[20] = listOfWords.index(word) + 1
            elif word == '-z':
                results[21] = listOfWords.index(word) + 1
            elif word == '-q':
                results[22] = listOfWords.index(word) + 1
            elif word == '--recordSize':
                results[23] = listOfWords.index(word) + 1
            elif word == '--mapreduceMapMemory':
                results[24] = listOfWords.index(word) + 1
            elif word == '--mapreduceReduceMemory':
                results[25] = listOfWords.index(word) + 1
            elif word == '--amResourceMB':
                results[26] = listOfWords.index(word) + 1
            elif word == '--mapCpuVcores':
                results[27] = listOfWords.index(word) + 1
            elif word == '--reduceCpuVcores':
                results[28] = listOfWords.index(word) + 1
            elif word == '--mapIntensity':
                results[29] = listOfWords.index(word) + 1
            elif word == '--mapSortIntensity':
                results[30] = listOfWords.index(word) + 1
            elif word == '--reduceIntensity':
                results[31] = listOfWords.index(word) + 1
            elif word == '--reduceSortIntensity':
                results[32] = listOfWords.index(word) + 1
            elif word == '--combinerIntensity':
                results[33] = listOfWords.index(word) + 1


        if results[0] != 0:
            print "Memory: " + str(listOfWords[results[0]])
        if results[1] != 0:
            print "Core Count: " + str(listOfWords[results[1]])
        if results[2] != 0:
            print "Core Speed: " + str(listOfWords[results[2]])
        if results[3] != 0:
            print "Max Read Speed: " + str(listOfWords[results[3]])
        if results[4] != 0:
            print "Max Write Speed: " + str(listOfWords[results[4]])
        if results[5] != 0:
            print "Min Read Speed: " + str(listOfWords[results[5]])
        if results[6] != 0:
            print "Min Write Speed: " + str(listOfWords[results[6]])
        if results[7] != 0:
            print "Link Speed: " + str(listOfWords[results[7]])
        if results[8] != 0:
            print "Reducer Count: " + str(listOfWords[results[8]])
        if results[9] != 0:
            print "Application Size: " + str(listOfWords[results[9]])
        if results[10] != 0:
            print "Map Output Percent: " + str(listOfWords[results[10]])
        if results[11] != 0:
            print "Reduce Output Percent: " + str(listOfWords[results[11]])
        if results[12] != 0:
            print "Final Output Percent: " + str(listOfWords[results[12]])
        if results[13] != 0:
            print "mapreduce.input.fileinputformat.split.minsize: " + str(listOfWords[results[13]])
        if results[14] != 0:
            print "mapreduce.input.fileinputformat.split.maxsize: " + str(listOfWords[results[14]])
        if results[15] != 0:
            print "dfsBlocksize: " + str(listOfWords[results[15]])
        if results[16] != 0:
            print "mapreduce.map.sort.spill.percent: " + str(listOfWords[results[16]])
        if results[17] != 0:
            print "yarn.nodemanager.resource.memory-mb: " + str(listOfWords[results[17]])
        if results[18] != 0:
            print "mapreduce.task.io.sort.mb: " + str(listOfWords[results[18]])
        if results[19] != 0:
            print "mapreduce.task.io.sort.factor: " + str(listOfWords[results[19]])
        if results[20] != 0:
            print "mapreduce.reduce.shuffle.merge.percent: " + str(listOfWords[results[20]])
        if results[21] != 0:
            print "mapreduce.reduce.shuffle.input.buffer.percent: " + str(listOfWords[results[21]])
        if results[22] != 0:
            print "mapreduce.job.reduce.slowstart.completedmaps: " + str(listOfWords[results[22]])
        if results[23] != 0:
            print "record size: " + str(listOfWords[results[23]])
        if results[24] != 0:
            print "mapreduce.map.memory.mb: " + str(listOfWords[results[24]])
        if results[25] != 0:
            print "mapreduce.reduce.memory.mb: " + str(listOfWords[results[25]])
        if results[26] != 0:
            print "yarn.app.mapreduce.am.resource.mb: " + str(listOfWords[results[26]])
        if results[27] != 0:
            print "mapreduce.map.cpu.vcores: " + str(listOfWords[results[27]])
        if results[28] != 0:
            print "mapreduce.reduce.cpu.vcores: " + str(listOfWords[results[28]])
        if results[29] != 0:
            print "Map Intensity: " + str(listOfWords[results[29]])
        if results[30] != 0:
            print "Map Sort Intensity: " + str(listOfWords[results[30]])
        if results[31] != 0:
            print "Reduce Intensity: " + str(listOfWords[results[31]])
        if results[32] != 0:
            print "Reduce Sort Intensity: " + str(listOfWords[results[32]])
        if results[33] != 0:
            print "Combiner Intensity: " + str(listOfWords[results[33]])

        print "Cluster Cost: " + str(self.simID_clusterCost[str(self.bestPerformance.split(None, 1)[0])])

# Use a first order RSM (y = Bo + B1X1 + B2X2 + ... + BnXn, where n = number of res)

try:
    start_time = time.time()

    greedy = RecommendationEngine()
    numberOfConfigs = greedy.eliminate()
    bestPerf, bestCmd = greedy.tester()

    print "Recommendation Generation Time: " + str(time.time() - start_time) + " seconds"

    greedy.presentResult()
except IOError, e:
    print "Interrupted..."
    sys.exit()
