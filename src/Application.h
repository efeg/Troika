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

#ifndef APPLICATION_H_
#define APPLICATION_H_

#include <stdlib.h>
#include <queue>
#include <memory>
#include <map>
#include "MapReduceConf.h"
#include "EventTimeCompare.h"

// default resource requirement per application master
#define YARN_APP_MAPREDUCE_AM_RESOURCE_MB 1536			// Default amount of memory the MR AppMaster needs.
#define YARN_APP_MAPREDUCE_AM_RESOURCE_CPUVCORES 1 	// Default number of virtual CPU cores the MR AppMaster needs.
#define MAPREDUCE_MAP_CPU_VCORES 1						// The number of virtual cores required for each map task.
#define MAPREDUCE_REDUCE_CPU_VCORES 1					// The number of virtual cores required for each reduce task.
#define EXPECTED_RECORD_SIZE_BYTES 65536				// Determined using real experiment results (User configurable!!)
#define MAPREDUCE_MAP_MEMORY_MB 1024					// Memory required per map task
#define MAPREDUCE_REDUCE_MEMORY_MB 1024				// Memory required per reduce task

extern std::priority_queue<Event, std::vector<Event>, EventTimeCompare > eventsList;
extern double simulationTime;	// simulation time

struct reducerMergeWaiters{
	int numberOfChunksWaitingToBeMerged_;
	std::queue<size_t> chunkSize_;
};

struct spillInfo{
	int fsID_;
	int nodeEventType_;
	double size_;
};

struct reduceWriteInfo{
	int numPackets_;
	double totalSplitSize_;
	bool lastRecordExist_;
	double lastRecordSize_;
};

struct reducerWaiterData{
	double neededResQuantity_;
	int myLoc_;
	int attribute_;
	int fsID_;
	int outputEventType_;
	bool dataSent;	// flag to show whether the part has been shuffled
};

struct recordInfo{
	size_t eachRecordSize_;
	size_t lastRecordSize_;
	bool lastRecordExist_;
	int remainingRecordCount_;
};

struct transferInformation{
	int remainingNumberOfMapRecords_;
	bool lastRecordExist_;
	double lastRecordSize_;
};

class Application {
public:
	Application(size_t, size_t, double, double, double, double, double, double, double, int, int, std::vector<int>, size_t, MapReduceConf,
			size_t recordSize=EXPECTED_RECORD_SIZE_BYTES, int mapCpuVcores=MAPREDUCE_MAP_CPU_VCORES,
			int reduceCpuVcores=MAPREDUCE_REDUCE_CPU_VCORES, size_t mapreduceMapMemory=MAPREDUCE_MAP_MEMORY_MB,
			size_t mapreduceReduceMemory=MAPREDUCE_REDUCE_MEMORY_MB, size_t amResourceMB=YARN_APP_MAPREDUCE_AM_RESOURCE_MB,
			int amCpuVcores=YARN_APP_MAPREDUCE_AM_RESOURCE_CPUVCORES);
	virtual ~Application();

	size_t getMapperOutputSize(int fsID);

	bool isLastFileSpillReadyForMerge(int fsID) const;

	void saveSpillInfo(int fsID, int nodeEventType, double size);

	void saveReadyToShuffleInfo(int fsID, int nodeEventType, double size);

	void saveReducerCompletionWaiters(double neededResQuantity, int myDest, int attribute, int fsID, int outputEventType);

	size_t getApplicationId() const;

	size_t getApplicationOwnerId() const;

	size_t getApplicationSize() const;

	void notifyIncomingOnDiskMergeWrite(int nodeId, int redID);

	double getMapOutputVolume() const;

	double getReduceOutputVolume() const;

	double getFinalOutputVolume() const;

	const MapReduceConf& getMapReduceConfig() const;

	size_t getFileSplitSize(int);

	size_t getRemainingSplitsToBeProcessed() const;

	int getClientEventType() const;

	int getRmEventType() const;

	int getAmEventType() const;

	size_t getNumberOfMappers();

	size_t getAmResourceMb() const;

	int getamCpuVcores() const;

	size_t getMapreduceMapMemory() const;

	size_t getMapreduceReduceMemory() const;

	int getMapCpuVcores() const;

	int getReduceCpuVcores() const;

	int popFileSplitNodeExpectedEventType();

	int getFileSplitNodeExpectedEventType(int index) const;

	size_t getQueueId() const;

	void setAmEventType(int amEventType);

	void pushBackAmControlledMappers(const int& mapperLoc);

	void pushBackAmControlledReducers(const int& reducerLoc);

	int getAmControlledMappers(size_t index) const;

	int getAmControlledReducers(size_t index) const;

	int getSeizedMapCpuVcores() const;

	void setSeizedMapCpuVcores(int seizedMapCpuVcores);

	size_t getSeizedMapreduceMapMemory() const;

	void setSeizedMapreduceMapMemory(size_t seizedMapreduceMapMemory);

	size_t getSeizedMapreduceReduceMemory() const;

	void setSeizedMapreduceReduceMemory(size_t seizedMapreduceReduceMemory);

	int getSeizedReduceCpuVcores() const;

	void setSeizedReduceCpuVcores(int seizedReduceCpuVcores);

	size_t getTotalNumberOfMappers() const;

	size_t getNumberOfReadyMappers() const;

	void notifyWbCompleteMappers();

	bool shuffleReady();

	spillInfo popReadyToShuffleInfo();

	int getReducerLocation(size_t index) const;

	int addReducerLocations(const int& location);

	bool hasAllReducersCreated() const;

	size_t gettotalReducerCompletionWaiters() const;

	reducerWaiterData popReducerCompletionWaiters();

	bool isReducerCompletionWaitersFull() const;

	bool isReducerCompletionWaitersHasElement() const;

	reducerWaiterData getReducerCompletionWaiter(int index);

	size_t getShuffleMergeLimit() const;

	void notifyDataReception(int nodeId, int redID, int fsID);

	bool datareceivedFromAllMappers(int nodeId, int redID);

	void adddataWaitingToBeMerged_(int nodeId, size_t dataWaitingToBeMerged, int redID);

	int getNumberOfWaitingTobeMerged(int nodeId, int redID);

	size_t popMergeSize(int nodeId, int redID);

	void notifyReduceSpillReception(int nodeId, int redID);

	void notifyReduceWriteComplete(int nodeId, int redID);

	void notifyCompletedReducers();

	int getReduceSpillCount(int nodeId, int redID);

	int getReduceWriteCount(int nodeId, int redID);

	void addReleasedMapper(int releasedFsID);

	bool hasReleasedMapperResources(int fsID);

	size_t getTotalChunkSizeWaitingTobeMerged(int nodeId, int redID);

	bool checkIfAllReducersComplete() const;

	double getMapFinishTime() const;

	double getMapStartTime() const;

	double getReduceFinishTime() const;

	double getReduceStartTime() const;

	void setMapFinishTime(double mapFinishTime);

	void setMapStartTime(double mapStartTime);

	void setReduceFinishTime(double reduceFinishTime);

	void setReduceStartTime(double reduceStartTime);

	void setShuffleFinishTime(double shuffleFinishTime, int redID);

	double getShuffleStartTime(int redID);

	double getShuffleExperiStartTime(int index, int redID);

	void setShuffleStartTime(double shuffleStartTime, int redID);

	size_t getWbCompleted() const;

	double getMapIntensity() const;

	double getReduceIntensity() const;

	double getReduceSortIntensity() const;

	void addMapStartTime(int index, double mapStartTime);

	void addShuffleStartTime(int index, int redID, double mapStartTime);

	void addMapFinishTime(int index, double finishTime);

	double avgMapTime() const;

	bool isReducersRequested() const;

	void setReducersRequested(bool reducersRequested);

	void incCompletedMapperCount();

	int getCompletedMapperCount();

	double getTotalShuffleAvgAmongReducers();

	void addReduceStartTime(int index, double redStartTime);

	void addReduceFinishTime(int index, double finishTime);

	double avgReduceTime() const;

	bool checkMaxSingleShuffleLimitExceed(size_t resourceSize);

	bool addReadyForOnDiskMergeCount_(int redId);

	int subReadyForOnDiskMergeCount_(int redId);

	void reduceNumberOfChunksWaitingToBeMerged(int nodeId, int redID);

	void setReduceRecordCount(int redId, int count);

	int getReduceRecordCount(int redId);

	bool areAllReduceRecordComplete(int redId);

	void setMapRecordCount(int fsID, int count);

	int getMapRecordCount(int fsID);

	void setMapMergeRecordCount(int fsID, int count);

	void setMapMergeInfo(int fsID, int redID, int remainingNumberOfMapRecords, bool lastRecordExist, double lastRecordSize);

	int getMapMergeRecordCount(int fsID);

	bool areAllMapRecordComplete(int fsId);

	void addMapMergeReadySize(int fsId, size_t size);

	size_t getMapMergeReadySize(int fsId);

	recordInfo getRecordInfo(int fsId);

	void setRecordInfo(int id, size_t eachRecordSize, size_t lastRecordSize, bool lastRecordExist, int remainingMapRecordCount, bool isMapRecord);

	void decrementRecordInfoRemainingMapRecordCount(int fsId);

	recordInfo getReduceRecordInfo(int redId);

	void decrementRecordInfoRemainingReduceRecordCount(int redId);

	size_t popMergeGivenSize(int nodeId, int redID, int mergeCount);

	bool notifyMapMergeComplete(int fsId);

	void signalReducerCompletionWaiter(int vectorIndex);

	void clearFinishedReducerCompletionWaiters();

	bool hasAnyReducerCreated() const;

	bool isThereACombiner() const;

	void setCombinerIntensity(double combinerIntensity);

	double getCombinerIntensity() const;

	int getShufflePacketCount(int fsID, int redID);

	void setShufflePacketCount(int fsID, int redID, int count);

	bool areAllShufflePacketComplete(int fsId, int redID);

	bool incShuffleCollectedDataAmount(int fsID, int redID, double shuffleCollectedDataAmount);

	bool incShuffleFlushedDataAmount(int fsID, int redID);

	void setShuffledTotalDataForFsidRedid(int fsID, int redID, double dataSize);

	double getCombinerCompressionPercent() const;

	void setCombinerCompressionPercent(double combinerCompressionPercent);

	int getMapTransferInfo_remainingNumberOfMapRecords(int fsId);

	bool getMapTransferInfo_lastRecordExist(int fsId);

	size_t getMapTransferInfo_lastRecordSize(int fsId);

	void setMapTransferInfo(int fsId, int remainingNumberOfMapRecords, bool lastRecordExist, size_t lastRecordSize);

	void decrementmapTransferInfoNumberOfMapRecords(int fsId, int decrementAmount);

	int incBufferCompletedPacketCount(int fsId);

	void resetBufferCompletedPacketCount(int fsId);

	void addMergeStartTime(int index, double mergeStartTime);

	void addMergeFnStartTime(int index, double mergeFnStartTime);

	void addMergeFnFinishTime(int index, double mergeFnFinTime);

	int incMergeBufferCompletedPacketCount(int fsID, int redID);

	void resetMergeBufferCompletedPacketCount(int fsID, int redID);

	double getMergeFnTime(int index);

	int decrementMergeInfoNumberOfMapRecords(int fsID, int redID, int decrementAmount);

	int getMergeInfo_remainingNumberOfMapRecords(int fsID, int redID);

	bool getMergeInfo_lastRecordExist(int fsID, int redID);

	double getMergeInfo_lastRecordSize(int fsID, int redID);

	void setReducerPartitionSize(int fsID, double size);

	double getReducerPartitionSize(int fsID);

	double getTotalMapMergeTime(int index);

	void setShuffleReadStartTime(int index, double time);

	void setShuffleReadFinTime(int index, double time);

	double getShuffleReadTime(int index);

	void setShuffleWriteDataProperties(size_t id, int numberOfPackets, double totalSplitSize, bool lastRecordExist, double lastRecordSize);

	int getShuffleWriteDataProperties_NumPackets(size_t id);

	double getShuffleWriteDataProperties_DataSize(size_t id);

	bool getShuffleWriteDataProperties_lastRecordExist(size_t id);

	double getShuffleWriteDataProperties_lastRecordSize(size_t id);

	int incShuffleWriteDataCount(size_t id);

	int decrementShuffleWriteDataRecords(size_t id, int decrementAmount);

	int getShuffleWriteData_numPackets(size_t id);

	bool getShuffleWriteData_lastRecordExist(size_t id);

	double getShuffleWriteData_lastRecordSize(size_t id);

	void resethuffleWriteDataCount(size_t id);

	void decrementRecordInfoRemainingMapRecordCountAmount(int fsId, int amount);

	void decrementRecordInfoRemainingReduceRecordCountAmount(int redId, int amount);

	void setShuffleReadInfo(int fsID, int redID, int remainingNumberOfMapRecords, bool lastRecordExist, size_t lastRecordSize);

	int incShuffleReadBufferCompletedPacketCount(int fsID, int redID);

	int decrementShuffleReadNumberOfRecords(int fsID, int redID, int decrementAmount);

	int getShuffleReadInfo_remainingNumberOfMapRecords(int fsID, int redID);

	bool getShuffleReadInfo_lastRecordExist(int fsID, int redID);

	size_t getShuffleReadInfo_lastRecordSize(int fsID, int redID);

	void resetShuffleReadBufferCompletedPacketCount(int fsID, int redID);

	size_t getRecordSize() const;

	double getMapSortIntensity() const;

private:

	static size_t ID_;
	size_t applicationID_, applicationSize_, applicationOwnerID_;
	double mapIntensity_, mapSortIntensity_, reduceIntensity_, reduceSortIntensity_;
	double mapOutputVolume_, reduceOutputVolume_, finalOutputVolume_;
	int clientEventType_, rmEventType_;

	std::vector<int> fileSplitExpectedNodeEvents_;
	size_t queueId_;			// equivalent of mapreduce.job.queuename (determines which queue the job will be submitted to)
	MapReduceConf mapReduceConfig_;
	size_t recordSize_;
	size_t fileSplitSize_;									// Calculated using max(minimumSize, min(maximumSize, blockSize))
	size_t lastFileSplitSize_;								// the size of last split might be <= fileSplitSize_
	size_t remainingSplitsToBeProcessed_;					// still needs to be processed in a map task
	// needed map reduce resources per task
	int mapCpuVcores_, reduceCpuVcores_;
	size_t mapreduceMapMemory_, mapreduceReduceMemory_;
	// needed
	size_t amResourceMB_;						// The amount of memory the MR AppMaster needs.
	int amCpuVcores_;							// The number of virtual CPU cores the MR AppMaster needs.
	std::vector<int> amControlledMappers_, amControlledReducers_;
	int lastFileSplitExpectedNodeEvent_;
	size_t totalExpectedNumberofSpills_;		// For regular filesplits
	size_t totalExpectedNumberofSpillsLast_;	// For the last filesplit
	std::vector<spillInfo> spillInfo_, readyToShuffleInfo_;
	std::vector<reducerWaiterData> reducerCompletionWaiters_;
	size_t totalNumberOfMappers_;
	std::vector<int> reducerLocations_;
	std::map<std::pair<int,int>, int> reducerNodeID_receivedMapperDataCount_, reducerNodeID_reduceSpillCount_, reducerNodeID_reduceWriteCount_, fsID_redID_receivedShufflePacketCount;
	std::map<std::pair<int,int>, reducerMergeWaiters> reducerNodeID_dataWaitingToBeMerged_;
	std::vector<int> releasedMappers_;
	std::map<int, double> shuffleStartTime_, shuffleFinishTime_;	// Start/finish time for each reducer id
	std::map<size_t, reduceWriteInfo> reduceWriteid_datasize;
	std::map<int, double> mapStartTimes_, redStartTimes_, mergeStartTimes_, mergeFnStartTime_, mergeFnFinTime_, mapMergeTime_,shuffleReadTime_, shuffleReadFinTime_, shuffleReadStartTime_;
	std::map<std::pair<int,int>, double> shuffleStartTimes_, shuffle_collectedDataAmount_, shuffle_flushedDataAmount_, shuffledTotalDataForFsidRedid_;
	std::map<int, int> reduceMergeId_;								// Reducer event type vs reduce mergeId
	bool reducersRequested_;
	std::map<int, int> readyForOnDiskMergeCount_, redId_recordId_, redId_CompletedRecordId_, fsId_recordId_, fsId_CompletedRecordId_, fsId_mapMergeRecordId_;
	std::map<std::pair<int,int>, int> fsId_redID_recordId_, fsId_redID_CompletedPacketId_, bufferMergeCompletedPacketCount_, bufferShuffleReadCompletedPacketCount_;	// used before shuffle to read each packet
	std::map<int, size_t> fsId_mapMergeReadysize_;
	std::map<int, double> parititionSize_;
	std::map<int, recordInfo> fsID_recordInfo_;
	std::map<int, transferInformation> fsID_mapTransferInfo_;
	std::map<std::pair<int,int>, transferInformation> fsID_mapMergeInfo_, fsID_shuffleReadInfo_;
	std::map<int,int> bufferCompletedPacketCount_;					// Used in transferring filesplit from remote
	std::map<size_t, int> bufferShuffleWriteDataCount_;
	std::map<int, recordInfo> redID_reduceRecordInfo_;
	std::map<int, int> fsId_completedMergeCount_;
	bool isThereACombiner_;
	int completedMapperCount_;
	double combinerIntensity_, combinerCompressionPercent_, totalMapFinishTime_, totalReduceFinishTime_, totalShuffleFinishTime_;
	int numberOfCompletedReducers_;
	// seized map reduce resources per task
	int seized_mapCpuVcores_, seized_reduceCpuVcores_;
	size_t seized_mapreduceMapMemory_, seized_mapreduceReduceMemory_;
	bool imReadyToShuffle_;
	size_t shuffleMergeLimit_, wb_completed_;
	int amEventType_;
	double reduceStartTime_, reduceFinishTime_, mapStartTime_, mapFinishTime_;
};

#endif /* APPLICATION_H_ */
