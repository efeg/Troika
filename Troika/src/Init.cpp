// Copyright (c) 2014, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//     * Redistributions of source code must retain the above copyright
//       notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of the <organization> nor the
//       names of its contributors may be used to endorse or promote products
//       derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include "Init.h"
#include <fstream>
#include <sstream>
#include <string>

// Note: If you don't have boost library , please install it
// (e.g. in Ubuntu: sudo apt-get install libboost-all-dev)
// use -lboost_program_options
#include "boost/program_options.hpp"
#include <boost/assign/list_of.hpp> // for 'map_list_of()'

#define INFITESIMAL 0.000000001

using namespace std;
namespace po = boost::program_options;

Scheduler g_scheduler;
bool terminal_output_disabled=false;

Init::Init():memoryCapacityInBytes_(0), numberOfCores_(0),
		cpuSpeed_(0), maxReadSpeed_(0),
		maxWriteSpeed_(0), minReadSpeed_(0),
		minWriteSpeed_(0), linkCapacity_(0),
		applicationSize_(0), mapOutputPercent_(0),
		reduceOutputPercent_(0), finalOutputPercent_(0),
		mrInputFileinputformatSplitMinsize_(0), mrInputFileinputformatSplitMaxsize_(0),
		dfsBlocksize_(0), mapreduceMapSortSpillPercent_(0),
		nodemanagerResourceMemoryMB_(0), mapreduceTaskIOSortMb_(0),
		mapreducetaskIOSortFactor_(0), mapreduceReduceShuffleMergePercent_(0),
		mapreduceReduceShuffleInputBufferPercent_(0), mapreduce_job_reduce_slowstart_completedmaps_(0),
		recordSize_(0), mapreduceMapMemory_(0),
		mapreduceReduceMemory_(0), amResourceMB_(0),
		mapCpuVcores_(0), reduceCpuVcores_(0),
		mapIntensity_(-1.0), mapSortIntensity_(-1.0),
		reduceIntensity_(-1.0), reduceSortIntensity_(-1.0),
		combinerIntensity_(-1.0){
}

void Init::initUserModules(string fileName, vector<std::shared_ptr<Module>>& eventExpectingModules, vector<std::shared_ptr<Application>>& applications, int reducerCount){
	ifstream infile(fileName);

	if (infile.good())
	{
		string line;
		size_t nodemanagerResManMB=0;
		queue<MapReduceConf> configurations;	// the order of "configurations" in the input file should be matched with the order of "applications"
		queue<Cpu> cpus;	// the order of "cpus" in the input file should be matched with the order of "nodes"
		queue<Memory> memories;	// the order of "memories" in the input file should be matched with the order of "nodes"
		queue<Harddisk> harddisks;	// the order of "harddisks" in the input file should be matched with the order of "nodes"
		vector <int> filesplitlocations;	// the expected event types of nodes where filesplits are located
		/*int numberofreducers = 1;*/
		size_t numberOfMappers = 0;

		while (getline(infile, line))
		{
			istringstream iss(line);
			char type;	// module type

			if(!(iss >> type) || type == '#'){
				// empty or comment line
		    	continue;
			}
			if(type == 'c' || type == 'C' ){	// MapReduceConf

				int mapreduceJobReduces;
			    if (!(iss >> mapreduceJobReduces )) {
			    	cerr << "ERROR: Missing/Corrupted user input in CONFIGURATIONS" << endl;
			    	exit(-1);
			    }

			    // set Troika configuration proposals
			    if(reducerCount!=-1){	// user typed reducer count from command line
			    	mapreduceJobReduces = reducerCount;
			    }

			    size_t mapreduceInputFileinputformatSplitMinsize = MAPREDUCE_INPUT_FILEINPUTFORMAT_SPLIT_MINSIZE;
				size_t mapreduceInputFileinputformatSplitMaxsize = MAX_SPLITSIZE;
				size_t dfsBlocksize = DFS_BLOCKSIZE;
				int mapreducetaskIOSortFactor = MAPREDUCE_TASK_IO_SORT_FACTOR;
				size_t mapreduceTaskIOSortMb = MAPREDUCE_TASK_IO_SORT_MB;
			    double mapreduceMapSortSpillPercent = MAPREDUCE_MAP_SORT_SPILL_PERCENT;
				int mapreduceJobtrackerTaskscheduler = CAPACITY;
			    int mapreduceReduceShuffleParallelcopies = MAPREDUCE_REDUCE_SHUFFLE_PARALLELCOPIES;
				double mapreduceReduceShuffleMergePercent = MAPREDUCE_REDUCE_SHUFFLEMERGE_PERCENT;
				double mapreduceReduceShuffleInputBufferPercent = MAPREDUCE_REDUCE_SHUFFLE_INPUT_BUFFER_PERCENT;
				double mapreduceReduceInputBufferPercent = MAPREDUCE_REDUCE_INPUT_BUFFER_PERCENT;
				bool mapreduceJobUbertaskEnable = MAPREDUCE_JOB_UBERTASK_ENABLE;
				int mapreduceJobUbertaskMaxmaps = MAPREDUCE_JOB_UBERTASK_MAXMAPS;
				int mapreduceJobUbertaskMaxreduces = MAPREDUCE_JOB_UBERTASK_MAXREDUCES;
				size_t nodemanagerResourceMemoryMB = YARN_NODEMANAGER_RESOURCE_MEMORY_MB;
			    int nodemanagerResourceCpuCores = YARN_NODEMANAGER_RESOURCE_CPU_CORES;
			    double mapreduce_job_reduce_slowstart_completedmaps = MAPREDUCE_JOB_REDUCE_SLOWSTART_COMPLETEDMAPS;
			    double mapred_child_java_opts;

			    if (!(iss >> mapreduceInputFileinputformatSplitMinsize) || !(iss >> mapreduceInputFileinputformatSplitMaxsize) || !(iss >> dfsBlocksize)
			    		|| !(iss >> mapreducetaskIOSortFactor) || !(iss >> mapreduceTaskIOSortMb) || !(iss >> mapreduceMapSortSpillPercent) || !(iss >> mapreduceJobtrackerTaskscheduler)
			    		|| !(iss >> mapreduceReduceShuffleParallelcopies) || !(iss >> mapreduceReduceShuffleMergePercent) || !(iss >> mapreduceReduceShuffleInputBufferPercent)
			    		|| !(iss >> mapreduceReduceInputBufferPercent) || !(iss >> mapreduceJobUbertaskEnable) || !(iss >> mapreduceJobUbertaskMaxmaps)
			    		|| !(iss >> mapreduceJobUbertaskMaxreduces) || !(iss >> nodemanagerResourceMemoryMB>> nodemanagerResourceCpuCores)
			    		|| !(iss >> mapreduce_job_reduce_slowstart_completedmaps) || !(iss >> mapred_child_java_opts)){


			    	// START: SET TROIKA PARAMS for 'c'
				    if(mrInputFileinputformatSplitMinsize_){
				    	mapreduceInputFileinputformatSplitMinsize = mrInputFileinputformatSplitMinsize_;
					}
				    if(mrInputFileinputformatSplitMaxsize_){
				    	mapreduceInputFileinputformatSplitMaxsize = mrInputFileinputformatSplitMaxsize_;
					}
				    if(dfsBlocksize_){
				    	dfsBlocksize = dfsBlocksize_;
					}
				    if(mapreducetaskIOSortFactor_){
				    	mapreducetaskIOSortFactor = mapreducetaskIOSortFactor_;
				    }
				    if(mapreduceTaskIOSortMb_){
				    	mapreduceTaskIOSortMb = mapreduceTaskIOSortMb_;
				    }
				    if(mapreduceMapSortSpillPercent_){
				    	mapreduceMapSortSpillPercent = mapreduceMapSortSpillPercent_;
				    }
				    if(mapreduceReduceShuffleMergePercent_){
				    	mapreduceReduceShuffleMergePercent = mapreduceReduceShuffleMergePercent_;
				    }
				    if(mapreduceReduceShuffleInputBufferPercent_){
				    	mapreduceReduceShuffleInputBufferPercent = mapreduceReduceShuffleInputBufferPercent_;
				    }
				    if(nodemanagerResourceMemoryMB_){
				    	nodemanagerResourceMemoryMB = nodemanagerResourceMemoryMB_;
				    }
				    if(mapreduce_job_reduce_slowstart_completedmaps_){
				    	mapreduce_job_reduce_slowstart_completedmaps = mapreduce_job_reduce_slowstart_completedmaps_;
				    }
			    	// END: SET TROIKA PARAMS for 'c'
			    	configurations.push(MapReduceConf(mapreduceJobReduces, mapreduceInputFileinputformatSplitMinsize, mapreduceInputFileinputformatSplitMaxsize,
			    			dfsBlocksize, mapreducetaskIOSortFactor, mapreduceTaskIOSortMb, mapreduceMapSortSpillPercent, static_cast<ResourceScheduler>(mapreduceJobtrackerTaskscheduler),
			    			mapreduceReduceShuffleParallelcopies, mapreduceReduceShuffleMergePercent, mapreduceReduceShuffleInputBufferPercent, mapreduceReduceInputBufferPercent,
			    			mapreduceJobUbertaskEnable, mapreduceJobUbertaskMaxmaps, mapreduceJobUbertaskMaxreduces, nodemanagerResourceMemoryMB, nodemanagerResourceCpuCores,
			    			mapreduce_job_reduce_slowstart_completedmaps));

					// nodemanagerResourceMemoryMB is set in input file
					nodemanagerResManMB = nodemanagerResourceMemoryMB;

			    	continue;
			    }

		    	// START: SET TROIKA PARAMS for 'c'
			    if(mrInputFileinputformatSplitMinsize_){
			    	mapreduceInputFileinputformatSplitMinsize = mrInputFileinputformatSplitMinsize_;
				}
			    if(mrInputFileinputformatSplitMaxsize_){
			    	mapreduceInputFileinputformatSplitMaxsize = mrInputFileinputformatSplitMaxsize_;
				}
			    if(dfsBlocksize_){
			    	dfsBlocksize = dfsBlocksize_;
				}
			    if(mapreducetaskIOSortFactor_){
			    	mapreducetaskIOSortFactor = mapreducetaskIOSortFactor_;
			    }
			    if(mapreduceTaskIOSortMb_){
			    	mapreduceTaskIOSortMb = mapreduceTaskIOSortMb_;
			    }
			    if(mapreduceMapSortSpillPercent_){
			    	mapreduceMapSortSpillPercent = mapreduceMapSortSpillPercent_;
			    }
			    if(mapreduceReduceShuffleMergePercent_){
			    	mapreduceReduceShuffleMergePercent = mapreduceReduceShuffleMergePercent_;
			    }
			    if(mapreduceReduceShuffleInputBufferPercent_){
			    	mapreduceReduceShuffleInputBufferPercent = mapreduceReduceShuffleInputBufferPercent_;
			    }
			    if(nodemanagerResourceMemoryMB_){
			    	nodemanagerResourceMemoryMB = nodemanagerResourceMemoryMB_;
			    }
			    if(mapreduce_job_reduce_slowstart_completedmaps_){
			    	mapreduce_job_reduce_slowstart_completedmaps = mapreduce_job_reduce_slowstart_completedmaps_;
			    }
		    	// END: SET TROIKA PARAMS for 'c'

		    	configurations.push(MapReduceConf(mapreduceJobReduces, mapreduceInputFileinputformatSplitMinsize, mapreduceInputFileinputformatSplitMaxsize,
		    			dfsBlocksize, mapreducetaskIOSortFactor, mapreduceTaskIOSortMb, mapreduceMapSortSpillPercent, static_cast<ResourceScheduler>(mapreduceJobtrackerTaskscheduler),
		    			mapreduceReduceShuffleParallelcopies, mapreduceReduceShuffleMergePercent, mapreduceReduceShuffleInputBufferPercent, mapreduceReduceInputBufferPercent,
		    			mapreduceJobUbertaskEnable, mapreduceJobUbertaskMaxmaps, mapreduceJobUbertaskMaxreduces, nodemanagerResourceMemoryMB, nodemanagerResourceCpuCores,
		    			mapreduce_job_reduce_slowstart_completedmaps, mapred_child_java_opts));

				// nodemanagerResourceMemoryMB is set in input file
				nodemanagerResManMB = nodemanagerResourceMemoryMB;

			}

			else if(type == 'f' || type == 'F' ){	// Filesplit locations

				int isforcedMapTaskCount, numberOfSplits;
			    if (!(iss >> isforcedMapTaskCount >> numberOfSplits)) {
			    	cerr << "ERROR: Missing/Corrupted user input in FileSplit Metadata" << endl;
			    	exit(-1);
			    }

			    // set filesplit size if number of map tasks is explicitly set within mapreduce code.
			    if(isforcedMapTaskCount){
			    	numberOfMappers = numberOfSplits;
			    }

			    int expectedNodeEventType;
				for(int i=0;i<numberOfSplits;i++){
					iss >> expectedNodeEventType;
					filesplitlocations.push_back(expectedNodeEventType);
				}
			}

			else if(type == 'r' || type == 'R'){	// Scheduler Config

				int numberOfQueues;
			    if (!(iss >> numberOfQueues )) {
			    	cerr << "ERROR: Missing/Corrupted user input in scheduler Configuration" << endl;
			    	exit(-1);
			    }

				int capacityOfQueue;
				std::vector<double> queueCapacities_;
				for(int i=0;i<numberOfQueues ;i++){
					iss >> capacityOfQueue;

					queueCapacities_.push_back(capacityOfQueue);
				}

				g_scheduler.setQueueCapacities(queueCapacities_);

				double maxAmResourcePercent;
			    if (!(iss >> maxAmResourcePercent )) {
					continue;
			    }

				g_scheduler.setMaxAmResourcePercent(maxAmResourcePercent);

				size_t minAllocMB, maxAllocMB;
				if(!(iss >> minAllocMB >> maxAllocMB)){
					continue;
				}

				g_scheduler.setMinAllocMb(minAllocMB);
				g_scheduler.setMaxAllocMb(maxAllocMB);

				int minAllocVcores, maxAllocVcores;
				if(!(iss >> minAllocVcores >> maxAllocVcores)){
					continue;
				}

				g_scheduler.setMinAllocVcores(minAllocVcores);
				g_scheduler.setMaxAllocVcores(maxAllocVcores);

				bool resourceCalculator;
				if(!(iss >> resourceCalculator)){
					continue;
				}

				g_scheduler.setResourceCalculator(resourceCalculator);
			}

			else if(type == 'a' || type == 'A' ){	// Application

				size_t applicationSize, applicationOwnerID;
				double mapIntensity, mapSortIntensity, reduceIntensity, reduceSortIntensity, mapOutputVolume, reduceOutputVolume, finalOutputVolume;
				int clientEventType, rmEventType;
				size_t queueId;

			    if (!(iss >> applicationSize >> applicationOwnerID >> mapIntensity >> mapSortIntensity >> reduceIntensity >> reduceSortIntensity >> mapOutputVolume >> reduceOutputVolume >> finalOutputVolume >> clientEventType >> rmEventType >> queueId)) {
			    	cerr << "ERROR: Missing/Corrupted user input in APPLICATION Module" << endl;
			    	exit(-1);
			    }

		    	// START: SET TROIKA PARAMS for 'a'
			    // following will be set if there is any external input for applicationSize (e.g. from recommendation engine)
				if(applicationSize_){
					applicationSize = applicationSize_;
				}
				if(mapIntensity_>= 0){
					mapIntensity = mapIntensity_;
				}
				if(mapSortIntensity_>= 0){
					mapSortIntensity = mapSortIntensity_;
				}
				if(reduceIntensity_>= 0){
					reduceIntensity = reduceIntensity_;
				}
				if(reduceSortIntensity_>= 0){
					reduceSortIntensity = reduceSortIntensity_;
				}
				if(mapOutputPercent_){
					mapOutputVolume = mapOutputPercent_;
				}
				if(reduceOutputPercent_){
					reduceOutputVolume = reduceOutputPercent_;
				}
				if(finalOutputPercent_){
					finalOutputVolume = finalOutputPercent_;
				}
		    	// END: SET TROIKA PARAMS for 'a'

			    if(!reduceIntensity){
			    	reduceIntensity = INFITESIMAL;
			    }
			    if(!reduceSortIntensity){
			    	reduceSortIntensity = INFITESIMAL;
			    }
			    if(!mapSortIntensity){
			    	mapSortIntensity = INFITESIMAL;
			    }
			    if(numberOfMappers){

			    	// if application size less than the number of mappers, then app size = number of mappers
			    	// the last file split size is application size - (number of mappers-1)*(application size/number of mappers)
			    	if(applicationSize < numberOfMappers){
			    		applicationSize = numberOfMappers;
			    	}

			    	size_t fsSize;
			    	if(applicationSize % numberOfMappers){	// there is leftover
			    		fsSize = (applicationSize / numberOfMappers) +1;
			    	}
			    	else{	// no leftover
			    		fsSize = applicationSize / numberOfMappers;
			    	}
				    configurations.front().setFileSplitSize(fsSize);
			    }

			    // Used to SET filesplit locations correctly in case the FS size is explicitly changed by TROIKA
			    // In case the filesplit size is changed by a compute intensive application (such as PI), then that change overwrites
			    // TROIKA's filesplit size modification
			    size_t fsSize = configurations.front().getFileSplitSize();
			    size_t numOfMappers = applicationSize/fsSize;
			    if(applicationSize%fsSize){
			    	numOfMappers++;
			    }

			    if(numOfMappers != filesplitlocations.size()){
			    	int genericLocation = filesplitlocations.front();
			    	filesplitlocations.clear();

					for(size_t i=0;i<numOfMappers;i++){
						filesplitlocations.push_back(genericLocation);
					}
			    }

			    size_t recordSize = EXPECTED_RECORD_SIZE_BYTES;
				int mapCpuVcores = MAPREDUCE_MAP_CPU_VCORES;
				int reduceCpuVcores = MAPREDUCE_REDUCE_CPU_VCORES;
			    size_t mapreduceMapMemory = MAPREDUCE_MAP_MEMORY_MB;
			    size_t mapreduceReduceMemory = MAPREDUCE_REDUCE_MEMORY_MB;
				size_t amResourceMB = YARN_APP_MAPREDUCE_AM_RESOURCE_MB;
				int cpuVcores = YARN_APP_MAPREDUCE_AM_RESOURCE_CPUVCORES;
			    int isThereACombiner=0;
			    double combinerIntensity, combinerCompressionPercent;

				if(!(iss >> recordSize) || !(iss >> isThereACombiner >> combinerIntensity >> combinerCompressionPercent) || !(iss >> mapCpuVcores >> reduceCpuVcores) ||
						!(iss >> mapreduceMapMemory >> mapreduceReduceMemory) || !(iss >> amResourceMB >> cpuVcores)){

			    	// START: SET TROIKA PARAMS for 'a'
					if(recordSize_){
						recordSize = recordSize_;
					}
					if(combinerIntensity_>=0){
						combinerIntensity = combinerIntensity_;
						isThereACombiner = 1;
					}
					if(mapCpuVcores_){
						mapCpuVcores = mapCpuVcores_;
					}
					if(reduceCpuVcores_){
						reduceCpuVcores = reduceCpuVcores_;
					}
					if(mapreduceMapMemory_){
						mapreduceMapMemory = mapreduceMapMemory_;
					}
					if(mapreduceReduceMemory_){
						mapreduceReduceMemory = mapreduceReduceMemory_;
					}
			    	// END: SET TROIKA PARAMS for 'a'

				    applications.push_back(std::make_shared<Application>(applicationSize, applicationOwnerID, mapIntensity, mapSortIntensity, reduceIntensity, reduceSortIntensity, mapOutputVolume, reduceOutputVolume,
				    		finalOutputVolume, clientEventType, rmEventType, filesplitlocations, queueId, configurations.front(), recordSize, mapCpuVcores, reduceCpuVcores, mapreduceMapMemory,
				    		mapreduceReduceMemory));

				    configurations.pop();
					if(isThereACombiner){ // non-zero isThereACombiner means there is a combiner
						applications.back()->setCombinerIntensity(combinerIntensity);
						applications.back()->setCombinerCompressionPercent(combinerCompressionPercent);
					}
				    continue;
				}

		    	// START: SET TROIKA PARAMS for 'a'
				if(recordSize_){
					recordSize = recordSize_;
				}
				if(combinerIntensity_>=0){
					combinerIntensity = combinerIntensity_;
					isThereACombiner = 1;
				}
				if(mapCpuVcores_){
					mapCpuVcores = mapCpuVcores_;
				}
				if(reduceCpuVcores_){
					reduceCpuVcores = reduceCpuVcores_;
				}
				if(mapreduceMapMemory_){
					mapreduceMapMemory = mapreduceMapMemory_;
				}
				if(mapreduceReduceMemory_){
					mapreduceReduceMemory = mapreduceReduceMemory_;
				}
				if(amResourceMB_){
					amResourceMB = amResourceMB_;
				}
		    	// END: SET TROIKA PARAMS for 'a'

			    applications.push_back(std::make_shared<Application>(applicationSize, applicationOwnerID, mapIntensity, mapSortIntensity, reduceIntensity, reduceSortIntensity, mapOutputVolume, reduceOutputVolume,
			    		finalOutputVolume, clientEventType, rmEventType, filesplitlocations, queueId, configurations.front(), recordSize, mapCpuVcores, reduceCpuVcores, mapreduceMapMemory,
			    		mapreduceReduceMemory, amResourceMB, cpuVcores));
			    configurations.pop();
				if(isThereACombiner){ // non-zero isThereACombiner means there is a combiner
					applications.back()->setCombinerIntensity(combinerIntensity);
					applications.back()->setCombinerCompressionPercent(combinerCompressionPercent);
				}
			}
			else if(type == 'p' || type == 'P' ){	// CPU

				int numberOfCores;
				size_t capacity;
				if (!(iss >> numberOfCores >>  capacity)) {
					cerr << "ERROR: Missing/Corrupted user input in CPU" << endl;
					exit(-1);
				}
				if(numberOfCores_){
					numberOfCores = numberOfCores_;
				}
				if(cpuSpeed_){
					capacity = cpuSpeed_;
				}
				int delayType;
				if (!(iss >> delayType)){
					cpus.push(Cpu(numberOfCores, capacity));
					continue;
				}
				int unit;
				if (!(iss >> unit)){
					cpus.push(Cpu(numberOfCores, capacity, static_cast<DistributionType>(delayType)));
					continue;
				}
				double delayratio;
				if (!(iss >> delayratio)){
					cpus.push(Cpu(numberOfCores, capacity, static_cast<DistributionType>(delayType), static_cast<TimeType>(unit)));
					continue;
				}
				cpus.push(Cpu(numberOfCores, capacity, static_cast<DistributionType>(delayType), static_cast<TimeType>(unit), delayratio));
			}

			else if(type == 'm' || type == 'M' ){	// Memory

				size_t maxReadSpeed, maxWriteSpeed, minReadSpeed, minWriteSpeed, remainingCapacity;
				if (!(iss >> maxReadSpeed >> maxWriteSpeed >> minReadSpeed >> minWriteSpeed >> remainingCapacity)) {
					cerr << "ERROR: Missing/Corrupted user input in MEMORY" << endl;
					exit(-1);
				}
				if(memoryCapacityInBytes_){
					remainingCapacity = memoryCapacityInBytes_;
				}
				int delayType;
				if (!(iss >> delayType)){
					memories.push(Memory(maxReadSpeed, maxWriteSpeed, minReadSpeed, minWriteSpeed, remainingCapacity));
					continue;
				}
				int unit;
				if (!(iss >> unit)){
					memories.push(Memory(maxReadSpeed, maxWriteSpeed, minReadSpeed, minWriteSpeed, remainingCapacity, static_cast<DistributionType>(delayType)));
					continue;
				}
				double delayratio;
				if (!(iss >> delayratio)){
					memories.push(Memory(maxReadSpeed, maxWriteSpeed, minReadSpeed, minWriteSpeed, remainingCapacity, static_cast<DistributionType>(delayType), static_cast<TimeType>(unit)));
					continue;
				}

				memories.push(Memory(maxReadSpeed, maxWriteSpeed, minReadSpeed, minWriteSpeed, remainingCapacity, static_cast<DistributionType>(delayType), static_cast<TimeType>(unit), delayratio));
			}
			else if(type == 'h' || type == 'H' ){	// HardDisk

				size_t maxReadSpeed, maxWriteSpeed, minReadSpeed, minWriteSpeed;
				if (!(iss >> maxReadSpeed >> maxWriteSpeed >> minReadSpeed >> minWriteSpeed)) {
					cerr << "ERROR: Missing/Corrupted user input in HARDDISK" << endl;
					exit(-1);
				}

				if(maxReadSpeed_){
					maxReadSpeed = maxReadSpeed_;
				}
				if(maxWriteSpeed_){
					maxWriteSpeed = maxWriteSpeed_;
				}
				if(minReadSpeed_){
					minReadSpeed = minReadSpeed_;
				}
				if(minWriteSpeed_){
					minWriteSpeed = minWriteSpeed_;
				}

				int delayType;
				if (!(iss >> delayType)){
					harddisks.push(Harddisk(maxReadSpeed, maxWriteSpeed, minReadSpeed, minWriteSpeed));
					continue;
				}
				int unit;
				if (!(iss >> unit)){
					harddisks.push(Harddisk(maxReadSpeed, maxWriteSpeed, minReadSpeed, minWriteSpeed, static_cast<DistributionType>(delayType)));
					continue;
				}
				double delayratio;
				if (!(iss >> delayratio)){
					harddisks.push(Harddisk(maxReadSpeed, maxWriteSpeed, minReadSpeed, minWriteSpeed, static_cast<DistributionType>(delayType), static_cast<TimeType>(unit)));
					continue;
				}
				harddisks.push(Harddisk(maxReadSpeed, maxWriteSpeed, minReadSpeed, minWriteSpeed, static_cast<DistributionType>(delayType), static_cast<TimeType>(unit), delayratio));
			}
			else if(type == 'n' || type == 'N' ){	// Node

				int rackExpectedEventType, nodeType, expectedEventType, outputEventType;

				if (!(iss >> rackExpectedEventType >> nodeType >> expectedEventType >> outputEventType)) {
					cerr << "ERROR: Missing/Corrupted user input in NODE" << endl;
					exit(-1);
				}
				eventExpectingModules.push_back(std::make_shared<Node>(rackExpectedEventType, static_cast<NodeType>(nodeType), expectedEventType, cpus.front(), harddisks.front(),
			    		memories.front(), outputEventType));

				if(!nodemanagerResManMB){
					nodemanagerResManMB = YARN_NODEMANAGER_RESOURCE_MEMORY_MB;
				}
				if(static_cast<NodeType>(nodeType) == RESOURCEMANAGER){
					g_scheduler.updateQueueRemainingCapacities(expectedEventType, cpus.front().getRemainingNumberOfCores(), memories.front().getRemainingCapacity(), true, nodemanagerResManMB);
				}
				else{
					// every node can keep a filesplit but RM's node cannot have a nodemanager
					g_scheduler.updateQueueRemainingCapacities(expectedEventType, cpus.front().getRemainingNumberOfCores(), memories.front().getRemainingCapacity(), false, nodemanagerResManMB);
				}

				cpus.pop();
			    harddisks.pop();
			    memories.pop();
			}
			else if(type == 's' || type == 'S' ){	// Switch

				int expectedEventType;

				if (!(iss >> expectedEventType)) {
					cerr << "ERROR: Missing/Corrupted user input in NODE" << endl;
					exit(-1);
				}
				int masterLinkType;
				if (!(iss >> masterLinkType)){
					eventExpectingModules.push_back(std::make_shared<Switch>(expectedEventType));
					continue;
				}

				eventExpectingModules.push_back(std::make_shared<Switch>(expectedEventType, masterLinkType));
			}
			else if(type == 'l' || type == 'L' ){	// Link

				double capacity;
				int expectedEventType, masterEventType, workerEventType;

				if (!(iss >> capacity >> expectedEventType >> masterEventType >> workerEventType)) {
					cerr << "ERROR: Missing/Corrupted user input in LINK" << endl;
					exit(-1);
				}
				if(linkCapacity_){
					capacity = linkCapacity_;
				}
				double mttr;
				if (!(iss >> mttr)){
					eventExpectingModules.push_back(std::make_shared<Link>(capacity, expectedEventType, masterEventType, workerEventType));
					continue;
				}
				int mtbf;
				if (!(iss >> mtbf)){
					eventExpectingModules.push_back(std::make_shared<Link>(capacity, expectedEventType, masterEventType, workerEventType, mttr));
					continue;
				}

				int delayType;
				if (!(iss >> delayType)){
					eventExpectingModules.push_back(std::make_shared<Link>(capacity, expectedEventType, masterEventType, workerEventType, mttr, mtbf));
					continue;
				}
				int unit;
				if (!(iss >> unit)){
					eventExpectingModules.push_back(std::make_shared<Link>(capacity, expectedEventType, masterEventType, workerEventType, mttr, mtbf,
							static_cast<DistributionType>(delayType)));
					continue;
				}

				double delayratio;
				if (!(iss >> delayratio)){
					eventExpectingModules.push_back(std::make_shared<Link>(capacity, expectedEventType, masterEventType, workerEventType, mttr, mtbf,
							static_cast<DistributionType>(delayType), static_cast<TimeType>(unit)));
					continue;
				}

				eventExpectingModules.push_back(std::make_shared<Link>(capacity, expectedEventType, masterEventType, workerEventType, mttr, mtbf,
											static_cast<DistributionType>(delayType), static_cast<TimeType>(unit), delayratio));
			}
		}
	}
}

void Init::initDES(double *endingCondition, int argc, char* argv[], vector<std::shared_ptr<Module>>& eventExpectingModules, vector<std::shared_ptr<Application>>& applications){

	// Declare the supported options.
		po::options_description desc("Allowed options");
		desc.add_options()		// a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z
		    ("help", "produce help message")
		    ("ending,e", po::value<double>()->default_value(9999999999999999.0),	"set ending time")
		    ("time-type,t", po::value<string>()->default_value("second"),			"select an ending time type: second | minute | hour")
			("input,i", po::value<string>()->default_value("input.txt"),			"input file name to create modules")
			("reducerCount,r", po::value<int>()->default_value(-1),				"set reducer count")
			("memoryCapacityInBytes,m", po::value<size_t>()->default_value(0),		"set memory capacity in bytes")
			("numberOfCores,c", po::value<int>()->default_value(0),				"set number of cores")
			("cpuSpeed,s", po::value<size_t>()->default_value(0),					"set cpu speed")
			("maxReadSpeed,v", po::value<size_t>()->default_value(0),				"set max ReadSpeed")
			("maxWriteSpeed,w", po::value<size_t>()->default_value(0),				"set max WriteSpeed")
			("minReadSpeed,x", po::value<size_t>()->default_value(0),				"set min ReadSpeed")
			("minWriteSpeed,y", po::value<size_t>()->default_value(0),				"set min WriteSpeed")
		    ("linkCapacity,l", po::value<double>()->default_value(0),				"set link capacity")
		    ("applicationSize,a", po::value<size_t>()->default_value(0),			"set application size")
		    ("mapOutputPercent,p", po::value<double>()->default_value(0),			"map output percent")
		    ("reduceOutputPercent,u", po::value<double>()->default_value(0),		"reduce output percent")
		    ("finalOutputPercent,f", po::value<double>()->default_value(0),		"final output percent")
			("mrInputFileinputformatSplitMinsize,b", po::value<size_t>()->default_value(0),	"set mapreduce.input.fileinputformat.split.minsize")
			("mrInputFileinputformatSplitMaxsize,d", po::value<size_t>()->default_value(0),	"set mapreduce.input.fileinputformat.split.maxsize")
			("dfsBlocksize,g", po::value<size_t>()->default_value(0),				"set dfsBlocksize")
		    ("mapreduceMapSortSpillPercent,h", po::value<double>()->default_value(0),		"set mapreduce.map.sort.spill.percent")
			("nodemanagerResourceMemoryMB,j", po::value<size_t>()->default_value(0),		"set nodemanagerResourceMemoryMB")
			("mapreduceTaskIOSortMb,k", po::value<size_t>()->default_value(0),				"set mapreduceTaskIOSortMb")
			("mapreducetaskIOSortFactor,n", po::value<int>()->default_value(0),			"set mapreducetaskIOSortFactor")
		    ("mapreduceReduceShuffleMergePercent,o", po::value<double>()->default_value(0),			"set mapreduceReduceShuffleMergePercent")
		    ("mapreduceReduceShuffleInputBufferPercent,z", po::value<double>()->default_value(0),		"set mapreduceReduceShuffleInputBufferPercent")
		    ("mapreduce_job_reduce_slowstart_completedmaps,q", po::value<double>()->default_value(0),	"set mapreduce_job_reduce_slowstart_completedmaps")
		    ("recordSize", po::value<size_t>()->default_value(0),				"set an average record size")
			("mapreduceMapMemory", po::value<size_t>()->default_value(0),		"set an average record size")
			("mapreduceReduceMemory", po::value<size_t>()->default_value(0),	"set an average record size")
			("amResourceMB", po::value<size_t>()->default_value(0),			"set an average record size")
			("mapCpuVcores", po::value<int>()->default_value(0),				"set number of virtual cores for a map task")
			("reduceCpuVcores", po::value<int>()->default_value(0),			"set number of virtual cores for a reduce task")
		    ("mapIntensity", po::value<double>()->default_value(-1.0),			"set Map Intensity")
		    ("mapSortIntensity", po::value<double>()->default_value(-1.0),		"set Map Sort Intensity")
		    ("reduceIntensity", po::value<double>()->default_value(-1.0),		"set Reduce Intensity")
		    ("reduceSortIntensity", po::value<double>()->default_value(-1.0),	"set Reduce Sort Intensity")
		    ("combinerIntensity", po::value<double>()->default_value(-1.0),	"set Combiner Intensity")
		;

		try{
			po::variables_map vm;
			po::store(po::parse_command_line(argc, argv, desc), vm);
			po::notify(vm);
			*endingCondition = vm["ending"].as<double>();

			if (vm.count("help")) {
				cout << desc << "\n";
				exit(1);
			}

			// arguments are received via command line...
			memoryCapacityInBytes_ = vm["memoryCapacityInBytes"].as<size_t>();
			numberOfCores_ = vm["numberOfCores"].as<int>();
			cpuSpeed_ = vm["cpuSpeed"].as<size_t>();
			maxReadSpeed_ = vm["maxReadSpeed"].as<size_t>();
			maxWriteSpeed_ = vm["maxWriteSpeed"].as<size_t>();
			minReadSpeed_ = vm["minReadSpeed"].as<size_t>();
			minWriteSpeed_ = vm["minWriteSpeed"].as<size_t>();
			linkCapacity_ = vm["linkCapacity"].as<double>();
			applicationSize_ = vm["applicationSize"].as<size_t>();
			mapOutputPercent_ = vm["mapOutputPercent"].as<double>();
			reduceOutputPercent_ = vm["reduceOutputPercent"].as<double>();
			finalOutputPercent_ = vm["finalOutputPercent"].as<double>();
			mrInputFileinputformatSplitMinsize_ = vm["mrInputFileinputformatSplitMinsize"].as<size_t>();
			mrInputFileinputformatSplitMaxsize_ = vm["mrInputFileinputformatSplitMaxsize"].as<size_t>();
			dfsBlocksize_ = vm["dfsBlocksize"].as<size_t>();
			mapreduceMapSortSpillPercent_ = vm["mapreduceMapSortSpillPercent"].as<double>();
			nodemanagerResourceMemoryMB_ = vm["nodemanagerResourceMemoryMB"].as<size_t>();
			mapreduceTaskIOSortMb_ = vm["mapreduceTaskIOSortMb"].as<size_t>();
			mapreducetaskIOSortFactor_ = vm["mapreducetaskIOSortFactor"].as<int>();
			mapreduceReduceShuffleMergePercent_ = vm["mapreduceReduceShuffleMergePercent"].as<double>();
			mapreduceReduceShuffleInputBufferPercent_ = vm["mapreduceReduceShuffleInputBufferPercent"].as<double>();
			mapreduce_job_reduce_slowstart_completedmaps_ = vm["mapreduce_job_reduce_slowstart_completedmaps"].as<double>();
			recordSize_ = vm["recordSize"].as<size_t>();
			mapreduceMapMemory_ = vm["mapreduceMapMemory"].as<size_t>();
			mapreduceReduceMemory_ = vm["mapreduceReduceMemory"].as<size_t>();
			amResourceMB_ = vm["amResourceMB"].as<size_t>();
			mapCpuVcores_ = vm["mapCpuVcores"].as<int>();
			reduceCpuVcores_ = vm["reduceCpuVcores"].as<int>();
			mapIntensity_ = vm["mapIntensity"].as<double>();
			mapSortIntensity_ = vm["mapSortIntensity"].as<double>();
			reduceIntensity_ = vm["reduceIntensity"].as<double>();
			reduceSortIntensity_ = vm["reduceSortIntensity"].as<double>();
			combinerIntensity_ = vm["combinerIntensity"].as<double>();

			// convert ending time to seconds from given timeType
			if (vm["time-type"].as<string>() == "minute"){
				*endingCondition *= MINUTE_IN_SEC;
			}
			else if(vm["time-type"].as<string>() == "hour"){
				*endingCondition *= HOUR_IN_SEC;
			}
			if(vm["reducerCount"].as<int>() != -1){	// reducer count is typed in command line
				// do not print to terminal...
				terminal_output_disabled = true;
				initUserModules(vm["input"].as<string>(), eventExpectingModules, applications, vm["reducerCount"].as<int>());
			}
			else{
				initUserModules(vm["input"].as<string>(), eventExpectingModules, applications);
			}

		}  catch ( const std::exception& e ) {
			std::cerr << e.what() << std::endl;
			exit(1);
		}
}
