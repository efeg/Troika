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

#include "MapReduceConf.h"
#include <iostream>

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

MapReduceConf::MapReduceConf(int mapreduceJobReduces, size_t mapreduceInputFileinputformatSplitMinsize, size_t mapreduceInputFileinputformatSplitMaxsize,
		size_t dfsBlocksize, int mapreducetaskIOSortFactor, size_t mapreduceTaskIOSortMb, double mapreduceMapSortSpillPercent, enum ResourceScheduler mapreduceJobtrackerTaskscheduler,
		int mapreduceReduceShuffleParallelcopies, double mapreduceReduceShuffleMergePercent, double mapreduceReduceShuffleInputBufferPercent,
		double mapreduceReduceInputBufferPercent, bool mapreduceJobUbertaskEnable, size_t mapreduceJobUbertaskMaxmaps, int mapreduceJobUbertaskMaxreduces,
		size_t nodemanagerResourceMemoryMB, int nodemanagerResourceCpuCores, double mapreduce_job_reduce_slowstart_completedmaps, double mapred_child_java_opts):
		mapreduceJobReduces_(mapreduceJobReduces), mapreduceInputFileinputformatSplitMinsize_(mapreduceInputFileinputformatSplitMinsize),
		mapreduceInputFileinputformatSplitMaxsize_(mapreduceInputFileinputformatSplitMaxsize), dfsBlocksize_(dfsBlocksize),
		mapreducetaskIOSortFactor_(mapreducetaskIOSortFactor), mapreduceTaskIOSortMb_(mapreduceTaskIOSortMb),
		mapreduceMapSortSpillPercent_(mapreduceMapSortSpillPercent), mapreduceJobtrackerTaskscheduler_(mapreduceJobtrackerTaskscheduler),
		mapreduceReduceShuffleParallelcopies_(mapreduceReduceShuffleParallelcopies), mapreduceReduceShuffleMergePercent_(mapreduceReduceShuffleMergePercent),
		mapreduceReduceShuffleInputBufferPercent_(mapreduceReduceShuffleInputBufferPercent), mapreduceReduceInputBufferPercent_(mapreduceReduceInputBufferPercent),
		mapreduceJobUbertaskEnable_(mapreduceJobUbertaskEnable), mapreduceJobUbertaskMaxmaps_(mapreduceJobUbertaskMaxmaps),
		mapreduceJobUbertaskMaxreduces_(mapreduceJobUbertaskMaxreduces), nodemanagerResourceMemoryMB_(nodemanagerResourceMemoryMB),
		nodemanagerResourceCpuCores_(nodemanagerResourceCpuCores), mapreduce_job_reduce_slowstart_completedmaps_(mapreduce_job_reduce_slowstart_completedmaps),
		mapred_child_java_opts_(mapred_child_java_opts){

	mapreduceJobUbertaskMaxbytes_ = (size_t)(dfsBlocksize/2);
	// Calculated using max(minimumSize, min(maximumSize, blockSize))
	fileSplitSize_ =  MAX(mapreduceInputFileinputformatSplitMinsize_,MIN(mapreduceInputFileinputformatSplitMaxsize_,dfsBlocksize_));
	maxSingleShuffleLimit_ = mapred_child_java_opts_*MAX_SINGLE_SHUFFLE_LIMIT_RATIO;
	maxSingleShuffleLimit_<<=20;
}

MapReduceConf::~MapReduceConf() {
}

size_t MapReduceConf::getFileSplitSize() const {
	return fileSplitSize_;
}

void MapReduceConf::setFileSplitSize(size_t fsSize){
	fileSplitSize_ = fsSize;
}

int MapReduceConf::getMapreduceJobReduces() const {
	return mapreduceJobReduces_;
}

enum ResourceScheduler MapReduceConf::getMapreduceJobtrackerTaskscheduler() const {
	return mapreduceJobtrackerTaskscheduler_;
}

bool MapReduceConf::isMapreduceJobUbertaskEnable() const {
	return mapreduceJobUbertaskEnable_;
}

size_t MapReduceConf::getMapreduceJobUbertaskMaxbytes() const {
	return mapreduceJobUbertaskMaxbytes_;
}

size_t MapReduceConf::getMapreduceJobUbertaskMaxmaps() const {
	return mapreduceJobUbertaskMaxmaps_;
}

int MapReduceConf::getMapreduceJobUbertaskMaxreduces() const {
	return mapreduceJobUbertaskMaxreduces_;
}

double MapReduceConf::getMapreduceMapSortSpillPercent() const {
	return mapreduceMapSortSpillPercent_;
}

double MapReduceConf::getMapreduceReduceInputBufferPercent() const {
	return mapreduceReduceInputBufferPercent_;
}

double MapReduceConf::getMapreduceReduceShuffleInputBufferPercent() const {
	return mapreduceReduceShuffleInputBufferPercent_;
}

double MapReduceConf::getMapreduceReduceShuffleMergePercent() const {
	return mapreduceReduceShuffleMergePercent_;
}

int MapReduceConf::getMapreduceReduceShuffleParallelcopies() const {
	return mapreduceReduceShuffleParallelcopies_;
}

int MapReduceConf::getMapreducetaskIoSortFactor() const {
	return mapreducetaskIOSortFactor_;
}

size_t MapReduceConf::getMapreduceTaskIoSortMb() const {
	return mapreduceTaskIOSortMb_;
}

int MapReduceConf::getNodemanagerResourceCpuCores() const {
	return nodemanagerResourceCpuCores_;
}

size_t MapReduceConf::getNodemanagerResourceMemoryMb() const {
	return nodemanagerResourceMemoryMB_;
}

double MapReduceConf::getMapreduceJobReduceSlowstartCompletedmaps() const {
	return mapreduce_job_reduce_slowstart_completedmaps_;
}

double MapReduceConf::getMapredChildJavaOpts() const {
	return mapred_child_java_opts_;
}

size_t MapReduceConf::getMaxSingleShuffleLimit() const {
	return maxSingleShuffleLimit_;
}
