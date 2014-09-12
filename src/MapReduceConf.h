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

#ifndef MAPREDUCECONF_H_
#define MAPREDUCECONF_H_
#define MAX_SPLITSIZE ((size_t)-1)

#include <stdlib.h>
#include "Datatypes.h"

#define YARN_NODEMANAGER_RESOURCE_MEMORY_MB 8192			// Amount of physical memory, in MB, that can be allocated for containers.
#define YARN_NODEMANAGER_RESOURCE_CPU_CORES 8				// Number of CPU cores that can be allocated for containers.
#define MAPREDUCE_JOB_UBERTASK_ENABLE false				// mapreduce.job.ubertask.enable
#define MAPREDUCE_JOB_UBERTASK_MAXMAPS 9					// mapreduce.job.ubertask.maxmaps
#define MAPREDUCE_JOB_UBERTASK_MAXREDUCES 1				// mapreduce.job.ubertask.maxreduces
#define DFS_BLOCKSIZE 134217728							// dfs.blocksize
#define MAPREDUCE_INPUT_FILEINPUTFORMAT_SPLIT_MINSIZE 0	// mapreduce.input.fileinputformat.split.minsize
#define MAPREDUCE_TASK_IO_SORT_MB 100						// mapreduce.task.io.sort.mb
#define MAPREDUCE_MAP_SORT_SPILL_PERCENT 0.8				// mapreduce.map.sort.spill.percent /* Note: must be over 0.5*/
#define MAPREDUCE_TASK_IO_SORT_FACTOR 10					// mapreduce.task.io.sort.factor
#define MAPREDUCE_JOB_REDUCE_SLOWSTART_COMPLETEDMAPS 0.05	// mapreduce.job.reduce.slowstart.completedmaps
// Note: In YARN, it is no longer necessary to adjust io.sort.record.percent!
#define MAPREDUCE_REDUCE_SHUFFLEMERGE_PERCENT 0.66			// mapreduce.reduce.shuffle.merge.percent
#define MAPREDUCE_REDUCE_SHUFFLE_INPUT_BUFFER_PERCENT 0.7	// mapreduce.reduce.shuffle.input.buffer.percent
#define MAPREDUCE_REDUCE_INPUT_BUFFER_PERCENT 0.0 			// mapreduce.reduce.input.buffer.percent				// TODO: integrate this parameter to simulation
#define MAPREDUCE_REDUCE_SHUFFLE_PARALLELCOPIES 5			// mapreduce.reduce.shuffle.parallelcopies				// TODO: integrate this parameter to simulation
#define MAPRED_CHILD_JAVA_OPTS 200							// mapred.child.java.opts Default: 200MB
#define MAX_SINGLE_SHUFFLE_LIMIT_RATIO 0.25				// maxSingleShuffleLimitRatio;

class MapReduceConf {
public:
	MapReduceConf(int mapreduceJobReduces, size_t mapreduceInputFileinputformatSplitMinsize=MAPREDUCE_INPUT_FILEINPUTFORMAT_SPLIT_MINSIZE, size_t mapreduceInputFileinputformatSplitMaxsize=MAX_SPLITSIZE,
			size_t dfsBlocksize=DFS_BLOCKSIZE, int mapreducetaskIOSortFactor=MAPREDUCE_TASK_IO_SORT_FACTOR, size_t mapreduceTaskIOSortMb=MAPREDUCE_TASK_IO_SORT_MB,
			double mapreduceMapSortSpillPercent=MAPREDUCE_MAP_SORT_SPILL_PERCENT, enum ResourceScheduler mapreduceJobtrackerTaskscheduler=CAPACITY,
			int mapreduceReduceShuffleParallelcopies=MAPREDUCE_REDUCE_SHUFFLE_PARALLELCOPIES, double mapreduceReduceShuffleMergePercent=MAPREDUCE_REDUCE_SHUFFLEMERGE_PERCENT,
			double mapreduceReduceShuffleInputBufferPercent=MAPREDUCE_REDUCE_SHUFFLE_INPUT_BUFFER_PERCENT, double mapreduceReduceInputBufferPercent=MAPREDUCE_REDUCE_INPUT_BUFFER_PERCENT,
			bool mapreduceJobUbertaskEnable=MAPREDUCE_JOB_UBERTASK_ENABLE, size_t mapreduceJobUbertaskMaxmaps=MAPREDUCE_JOB_UBERTASK_MAXMAPS, int mapreduceJobUbertaskMaxreduces=MAPREDUCE_JOB_UBERTASK_MAXREDUCES,
			size_t nodemanagerResourceMemoryMB=YARN_NODEMANAGER_RESOURCE_MEMORY_MB, int nodemanagerResourceCpuCores=YARN_NODEMANAGER_RESOURCE_CPU_CORES,
			double mapreduce_job_reduce_slowstart_completedmaps=MAPREDUCE_JOB_REDUCE_SLOWSTART_COMPLETEDMAPS, double mapred_child_java_opts=MAPRED_CHILD_JAVA_OPTS);
	virtual ~MapReduceConf();

	size_t getFsSize() const;

	int getMapreduceJobReduces() const;

	enum ResourceScheduler getMapreduceJobtrackerTaskscheduler() const;

	bool isMapreduceJobUbertaskEnable() const;

	size_t getMapreduceJobUbertaskMaxbytes() const;

	size_t getMapreduceJobUbertaskMaxmaps() const;

	int getMapreduceJobUbertaskMaxreduces() const;

	double getMapreduceMapSortSpillPercent() const;

	double getMapreduceReduceInputBufferPercent() const;

	double getMapreduceReduceShuffleInputBufferPercent() const;

	double getMapreduceReduceShuffleMergePercent() const;

	int getMapreduceReduceShuffleParallelcopies() const;

	int getMapreducetaskIoSortFactor() const;

	size_t getMapreduceTaskIoSortMb() const;

	int getNodemanagerResourceCpuCores() const;

	size_t getNodemanagerResourceMemoryMb() const;

	double getMapreduceJobReduceSlowstartCompletedmaps() const;

	double getMapredChildJavaOpts() const;

	size_t getMaxSingleShuffleLimit() const;

private:
	int mapreduceJobReduces_;								// The number of reduce tasks per job (default: 1).
	size_t mapreduceInputFileinputformatSplitMinsize_;		// The minimum size chunk that map input should be split into (default: 0).
	size_t mapreduceInputFileinputformatSplitMaxsize_;		// The maximum size chunk that map input should be split into (default: (size_t)-1)
	size_t dfsBlocksize_;									// The block size for new files, in bytes. (default: 67108864).
	int mapreducetaskIOSortFactor_;							// The number of streams to merge at once while sorting files. This determines the number of open file handles. (default: 10)
	size_t mapreduceTaskIOSortMb_;							// The total amount of buffer memory to use while sorting files, in megabytes (default: 100).
	double mapreduceMapSortSpillPercent_; 					// The soft limit in the serialization buffer. Once reached, a thread will begin to spill the contents to disk in the background. Note that collection will not block if this threshold is exceeded while a spill is already in progress (default: 0.80).
	enum ResourceScheduler mapreduceJobtrackerTaskscheduler_;	// TODO:The class responsible for scheduling the tasks (default: CAPACITY).
	int mapreduceReduceShuffleParallelcopies_;				// The default number of parallel transfers run by reduce during the shuffle phase (default: 5).
	double mapreduceReduceShuffleMergePercent_;				// The usage threshold at which an in-memory merge will be initiated, expressed as a percentage of the total memory allocated to storing in-memory map outputs, as defined by mapreduce.reduce.shuffle.input.buffer.percent. (default: 0.66)
	double mapreduceReduceShuffleInputBufferPercent_;		// The percentage of memory to be allocated from the maximum heap size to storing map outputs during the shuffle. (default: 0.70)
	double mapreduceReduceInputBufferPercent_;				// The percentage of memory- relative to the maximum heap size- to retain map outputs during the reduce. When the shuffle is concluded, any remaining map outputs in memory must consume less than this threshold before the reduce can begin. (default: 0.0)
	bool mapreduceJobUbertaskEnable_;						// Whether to enable the small-jobs "ubertask" optimization, which runs "sufficiently small" jobs sequentially within a single JVM. "Small" is defined by the following maxmaps, maxreduces, and maxbytes settings. Users may override this value. (default: false)
	size_t mapreduceJobUbertaskMaxmaps_;					// Threshold for number of maps, beyond which job is considered too big for the ubertasking optimization. Users may override this value, but only downward. (default: 9)
	size_t mapreduceJobUbertaskMaxreduces_;					// Threshold for number of reduces, beyond which job is considered too big for the ubertasking optimization. CURRENTLY THE CODE CANNOT SUPPORT MORE THAN ONE REDUCE and will ignore larger values. (Zero is a valid max, however.) Users may override this value, but only downward. (default: 1)
	size_t mapreduceJobUbertaskMaxbytes_;					// Threshold for number of input bytes, beyond which job is considered too big for the ubertasking optimization. If no value is specified, dfs.block.size is used as a default. Be sure to specify a default value in mapred-site.xml if the underlying filesystem is not HDFS. Users may override this value, but only downward. (default: (size_t)(0.5 * dfsBlocksize_))
	size_t fileSplitSize_;									// Calculated using max(minimumSize, min(maximumSize, blockSize))
	size_t nodemanagerResourceMemoryMB_;
	int nodemanagerResourceCpuCores_;
	double mapreduce_job_reduce_slowstart_completedmaps_;	// Fraction of the number of maps in the job which should be complete before reduces are scheduled for the app.
	double mapred_child_java_opts_;
	size_t maxSingleShuffleLimit_;
};

#endif /* MAPREDUCECONF_H_ */
