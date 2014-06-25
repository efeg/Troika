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

#include "Application.h"
#include <iostream>
#include <algorithm>

size_t Application::ID_ = 0;

Application::Application (size_t applicationSize, size_t applicationOwnerID, double mapIntensity, double mapSortIntensity, double reduceIntensity, double reduceSortIntensity,
							double mapOutputVolume, double reduceOutputVolume, double finalOutputVolume,
							int clientEventType, int rmEventType, std::vector<int> fileSplitExpectedNodeEvents,
							size_t queueId, MapReduceConf mapReduceConfig, size_t recordSize,
							int mapCpuVcores, int reduceCpuVcores,
							size_t mapreduceMapMemory, size_t mapreduceReduceMemory,
							size_t amResourceMB, int amCpuVcores):
							applicationID_(ID_++), applicationSize_(applicationSize),
							applicationOwnerID_(applicationOwnerID), mapIntensity_(mapIntensity),
							mapSortIntensity_(mapSortIntensity), reduceIntensity_(reduceIntensity),
							reduceSortIntensity_(reduceSortIntensity), mapOutputVolume_(mapOutputVolume),
							reduceOutputVolume_(reduceOutputVolume), finalOutputVolume_(finalOutputVolume),
							clientEventType_(clientEventType), rmEventType_(rmEventType),
							fileSplitExpectedNodeEvents_(fileSplitExpectedNodeEvents), queueId_(queueId),
							mapReduceConfig_(mapReduceConfig), recordSize_(recordSize),
							mapCpuVcores_(mapCpuVcores), reduceCpuVcores_(reduceCpuVcores),
							mapreduceMapMemory_(mapreduceMapMemory), mapreduceReduceMemory_(mapreduceReduceMemory),
							amResourceMB_(amResourceMB), amCpuVcores_(amCpuVcores),
							isThereACombiner_(false), completedMapperCount_(0),
							combinerIntensity_(0), combinerCompressionPercent_(0),
							totalMapFinishTime_(0), totalReduceFinishTime_(0),
							totalShuffleFinishTime_(0), numberOfCompletedReducers_(0),
							seized_mapCpuVcores_(0), seized_reduceCpuVcores_(0),
							seized_mapreduceMapMemory_(0), seized_mapreduceReduceMemory_(0),
							imReadyToShuffle_(false), shuffleMergeLimit_(0),
							wb_completed_(0), amEventType_(-1),
							reduceStartTime_(-1), reduceFinishTime_(-1),
							mapStartTime_(-1), mapFinishTime_(-1)
							 {
	releasedMappers_ = {};

	// get last filesplit event type
	lastFileSplitExpectedNodeEvent_ = fileSplitExpectedNodeEvents_.back();
	totalNumberOfMappers_ = fileSplitExpectedNodeEvents_.size();

	fileSplitSize_ = mapReduceConfig_.getFileSplitSize();
	lastFileSplitSize_ = applicationSize_ % fileSplitSize_;

	if (lastFileSplitSize_ == 0){
		remainingSplitsToBeProcessed_ = applicationSize_ / fileSplitSize_;
		lastFileSplitSize_ = fileSplitSize_;
	}
	else{
		remainingSplitsToBeProcessed_ = ((applicationSize_ - lastFileSplitSize_)/ fileSplitSize_) + 1;
	}

	// how many spills will be created
	size_t mapreduceTaskIOSortMb = mapReduceConfig_.getMapreduceTaskIoSortMb();

	if(fileSplitSize_ % (mapreduceTaskIOSortMb << 20) == 0){
		totalExpectedNumberofSpills_ = fileSplitSize_ / (mapreduceTaskIOSortMb << 20);
	}
	else{
		totalExpectedNumberofSpills_ = (fileSplitSize_ / (mapreduceTaskIOSortMb << 20)) +1;
	}

	if(lastFileSplitSize_ % (mapreduceTaskIOSortMb << 20) == 0){
		totalExpectedNumberofSpillsLast_= lastFileSplitSize_ / (mapreduceTaskIOSortMb << 20);
	}
	else{
		totalExpectedNumberofSpillsLast_= (lastFileSplitSize_ / (mapreduceTaskIOSortMb << 20)) +1;
	}

	// Create first event in eventslist

	/*
	 * Initial Application submission event arrives at time 0
	 * DOC: eventBehavior: SUBMIT_JOB means "Submit application" for Client Node
	 *
	 */

	// Application enters to the system (run application command is sent)
	Event newEvent(applicationID_, simulationTime, 0.0, 0.0, clientEventType_, clientEventType_,
			SUBMIT_JOB, APPLICATION);

	eventsList.push(newEvent);

	for(int i=0;i<mapReduceConfig_.getMapreduceJobReduces();i++){
		shuffleStartTime_[i]=-1;
		shuffleFinishTime_[i]=-1;
	}

	reducersRequested_ = false;
}

Application::~Application() {
}

void Application::saveReadyToShuffleInfo(int fsID, int nodeEventType, double size){
	readyToShuffleInfo_.push_back({fsID, nodeEventType, size});
}

void Application::saveSpillInfo(int fsID, int nodeEventType, double size){
	spillInfo_.push_back({fsID, nodeEventType, size});
}

void Application::saveReducerCompletionWaiters(double neededResQuantity, int myDest, int attribute, int fsID, int outputEventType){
	reducerCompletionWaiters_.push_back({neededResQuantity, myDest, attribute, fsID, outputEventType, false});
}

bool Application::hasReleasedMapperResources(int fsID){
	for(size_t i=0;i<releasedMappers_.size();i++){
		if(releasedMappers_.at(i) == fsID){
			return true;
		}
	}
	return false;
}

void Application::notifyWbCompleteMappers(){
	wb_completed_++;
}

void Application::addReleasedMapper(int releasedFsID){
	releasedMappers_.push_back(releasedFsID);
}

size_t Application::getNumberOfMappers(){
	if (lastFileSplitSize_ == fileSplitSize_){
		return applicationSize_/ fileSplitSize_;
	}
	return ((applicationSize_/ fileSplitSize_) +1);
}

size_t Application::getApplicationId() const {
	return applicationID_;
}

size_t Application::getApplicationOwnerId() const {
	return applicationOwnerID_;
}

size_t Application::getApplicationSize() const {
	return applicationSize_;
}

double Application::getMapOutputVolume() const {
	return mapOutputVolume_;
}

double Application::getReduceOutputVolume() const {
	return reduceOutputVolume_;
}

double Application::getFinalOutputVolume() const {
	return finalOutputVolume_;
}

const MapReduceConf& Application::getMapReduceConfig() const {
	return mapReduceConfig_;
}

size_t Application::getFileSplitSize(int fileSplitID) {
	if((size_t)fileSplitID == (fileSplitExpectedNodeEvents_.size() -1)){	// lastSplit
		return lastFileSplitSize_;
	}
	return fileSplitSize_;
}

size_t Application::getRemainingSplitsToBeProcessed() const {
	return remainingSplitsToBeProcessed_;
}

int Application::getClientEventType() const {
	return clientEventType_;
}

int Application::getRmEventType() const {
	return rmEventType_;
}

int Application::getAmEventType() const{
	return amEventType_;
}

int Application::getMapCpuVcores() const {
	return mapCpuVcores_;
}

int Application::getReduceCpuVcores() const {
	return reduceCpuVcores_;
}

size_t Application::getMapreduceMapMemory() const {
	return mapreduceMapMemory_;
}

size_t Application::getMapreduceReduceMemory() const {
	return mapreduceReduceMemory_;
}

size_t Application::getAmResourceMb() const {
	return amResourceMB_;
}

int Application::getamCpuVcores() const {
	return amCpuVcores_;
}

int Application::getFileSplitNodeExpectedEventType(int index) const{
	if((size_t)index < fileSplitExpectedNodeEvents_.size()){	// index exists
		return fileSplitExpectedNodeEvents_.at(index);
	}
	return -1;	// Empty
}

int Application::popFileSplitNodeExpectedEventType(){
	if(!(fileSplitExpectedNodeEvents_.empty())){	// not empty
		int expectedNodeEventType;
		expectedNodeEventType = fileSplitExpectedNodeEvents_.back();
		fileSplitExpectedNodeEvents_.pop_back();

		return expectedNodeEventType;
	}
	return -1;	// Empty
}

size_t Application::getQueueId() const {
	return queueId_;
}

void Application::setAmEventType(int amEventType) {
	amEventType_ = amEventType;
}

void Application::pushBackAmControlledMappers(const int& mapperLoc) {
	amControlledMappers_.push_back(mapperLoc);
}

void Application::pushBackAmControlledReducers(const int& reducerLoc) {
	amControlledReducers_.push_back(reducerLoc);
}

int Application::getAmControlledMappers(size_t index) const {
	if(index < amControlledMappers_.size()){
		return amControlledMappers_.at(index);
	}
	return -1;
}

int Application::getAmControlledReducers(size_t index) const {
	if(index < amControlledReducers_.size()){
		return amControlledReducers_.at(index);
	}
	return -1;
}

int Application::getSeizedMapCpuVcores() const {
	return seized_mapCpuVcores_;
}

void Application::setSeizedMapCpuVcores(int seizedMapCpuVcores) {
	seized_mapCpuVcores_ = seizedMapCpuVcores;
}

size_t Application::getSeizedMapreduceMapMemory() const {
	return seized_mapreduceMapMemory_;
}

void Application::setSeizedMapreduceMapMemory(size_t seizedMapreduceMapMemory) {
	seized_mapreduceMapMemory_ = seizedMapreduceMapMemory;
	shuffleMergeLimit_ = (mapReduceConfig_.getMapredChildJavaOpts()*(mapReduceConfig_.getMapreduceReduceShuffleInputBufferPercent()))*mapReduceConfig_.getMapreduceReduceShuffleMergePercent();
	shuffleMergeLimit_<<= 20;
}

size_t Application::getSeizedMapreduceReduceMemory() const {
	return seized_mapreduceReduceMemory_;
}

void Application::setSeizedMapreduceReduceMemory(size_t seizedMapreduceReduceMemory) {
	seized_mapreduceReduceMemory_ = seizedMapreduceReduceMemory;
}

int Application::getSeizedReduceCpuVcores() const {
	return seized_reduceCpuVcores_;
}

void Application::setSeizedReduceCpuVcores(int seizedReduceCpuVcores) {
	seized_reduceCpuVcores_ = seizedReduceCpuVcores;
}

bool Application::isLastFileSpillReadyForMerge(int fsID) const{
	// count number of spills of fsID
	size_t totalSplits=0;
    for(unsigned int i=0; i < spillInfo_.size();i++){
    	if ( (spillInfo_.at(i).fsID_ == fsID)){
    		totalSplits++;
    	}
    }

	if(fileSplitExpectedNodeEvents_.size() == (size_t)fsID+1){	// last filesplit
		if(totalExpectedNumberofSpillsLast_ == totalSplits){
			return true;
		}
		return false;
	}

	if(totalExpectedNumberofSpills_ == totalSplits){
		return true;
	}
	return false;
}

size_t Application::getMapperOutputSize(int fsID){
	// count number of spills of fsID
	size_t totalSize=0;
    for(unsigned int i=0; i < spillInfo_.size();i++){
    	if ( (spillInfo_.at(i).fsID_ == fsID)){
    		totalSize += (spillInfo_.at(i)).size_;
    	}
    }
    return totalSize;
}

size_t Application::getTotalNumberOfMappers() const {
	return totalNumberOfMappers_;
}

size_t Application::getNumberOfReadyMappers() const {
	return readyToShuffleInfo_.size();
}

void Application::incCompletedMapperCount(){
	completedMapperCount_++;
}

int Application::getCompletedMapperCount(){
	return completedMapperCount_;
}

bool Application::shuffleReady(){
	double minCompletedRatio = mapReduceConfig_.getMapreduceJobReduceSlowstartCompletedmaps();

	if(totalNumberOfMappers_*minCompletedRatio <= getNumberOfReadyMappers()){
		imReadyToShuffle_ = true;
	}
	return imReadyToShuffle_;
}

spillInfo Application::popReadyToShuffleInfo(){
	spillInfo temp = readyToShuffleInfo_.back();
	readyToShuffleInfo_.pop_back();
	return temp;
}

void Application::signalReducerCompletionWaiter(int vectorIndex){
	// add new redID that the data was shuffled to
	reducerCompletionWaiters_[vectorIndex].dataSent = true;
}

void Application::clearFinishedReducerCompletionWaiters(){
	reducerCompletionWaiters_.erase(
	    std::remove_if(
	    		reducerCompletionWaiters_.begin(),
	    		reducerCompletionWaiters_.end(),
	        [](reducerWaiterData element) -> bool {
				// if mapper output is shuffled to all reducers, remove it from waiting list
				if (element.dataSent){
					return true;
				}
				return false;
	        }
	    ),
	    reducerCompletionWaiters_.end()
	);
}

reducerWaiterData Application::popReducerCompletionWaiters(){
	reducerWaiterData temp = reducerCompletionWaiters_.back();
	reducerCompletionWaiters_.pop_back();
	return temp;
}

reducerWaiterData Application::getReducerCompletionWaiter(int index){
	return reducerCompletionWaiters_.at(index);
}

int Application::getReducerLocation(size_t index) const {
	if(index < reducerLocations_.size()){
		return reducerLocations_.at(index);
	}
	return -1;
}

int Application::addReducerLocations(const int& location) {
	reducerLocations_.push_back(location);
	if(hasAllReducersCreated()){
		return 1;
	}
	return 0;
}

bool Application::hasAllReducersCreated() const{
	if((size_t)(mapReduceConfig_.getMapreduceJobReduces()) == reducerLocations_.size()){
		return true;
	}
	return false;
}

bool Application::hasAnyReducerCreated() const{
	if(reducerLocations_.size() > 0){
		return true;
	}
	return false;
}

bool Application::isThereACombiner() const{
	return isThereACombiner_;
}

void Application::setCombinerIntensity(double combinerIntensity){
	isThereACombiner_ = true;
	combinerIntensity_ = combinerIntensity;
}

void Application::setCombinerCompressionPercent(double combinerCompressionPercent){
	combinerCompressionPercent_ = combinerCompressionPercent;
}

double Application::getCombinerIntensity() const {
	return combinerIntensity_;
}

double Application::getCombinerCompressionPercent() const {
	return combinerCompressionPercent_;
}

size_t Application::gettotalReducerCompletionWaiters() const{
	return reducerCompletionWaiters_.size();
}

bool Application::isReducerCompletionWaitersFull() const{
	if(reducerCompletionWaiters_.size() == (size_t)((mapReduceConfig_.getMapreduceJobReduces())*totalNumberOfMappers_)){
		return true;
	}
	return false;
}

bool Application::isReducerCompletionWaitersHasElement() const{
	if(reducerCompletionWaiters_.empty()){
		return false;
	}
	return true;
}

size_t Application::getShuffleMergeLimit() const {
	return shuffleMergeLimit_;
}

void Application::notifyDataReception(int nodeId, int redID, int fsID){
	if ( fsID_redID_receivedShufflePacketCount.find(std::make_pair(fsID,redID)) == fsID_redID_receivedShufflePacketCount.end() ) {	// not found
		fsID_redID_receivedShufflePacketCount[std::make_pair(fsID,redID)] = 1;
	}
	else {	// found
		fsID_redID_receivedShufflePacketCount[std::make_pair(fsID,redID)]++;
	}

	if(fsID_redID_receivedShufflePacketCount[std::make_pair(fsID,redID)] == fsId_redID_recordId_[std::make_pair(fsID,redID)] ){

		// increment map counter for nodeId
		if ( reducerNodeID_receivedMapperDataCount_.find(std::make_pair(nodeId,redID)) == reducerNodeID_receivedMapperDataCount_.end() ) {	// not found
			reducerNodeID_receivedMapperDataCount_[std::make_pair(nodeId,redID)] = 1;
		}
		else {	// found
			reducerNodeID_receivedMapperDataCount_[std::make_pair(nodeId,redID)]++;
		}
	}
}

void Application::notifyReduceSpillReception(int nodeId, int redID){
	// increment map counter for nodeId
	if ( reducerNodeID_reduceSpillCount_.find(std::make_pair(nodeId,redID)) == reducerNodeID_reduceSpillCount_.end() ) {	// not found
		reducerNodeID_reduceSpillCount_[std::make_pair(nodeId,redID)] = 1;
	}
	else {	// found
		reducerNodeID_reduceSpillCount_[std::make_pair(nodeId,redID)]++;
	}
}

void Application::notifyReduceWriteComplete(int nodeId, int redID){
	// increment counter for nodeId
	if ( reducerNodeID_reduceWriteCount_.find(std::make_pair(nodeId,redID)) == reducerNodeID_reduceWriteCount_.end() ) {	// not found
		reducerNodeID_reduceWriteCount_[std::make_pair(nodeId,redID)] = 1;
	}
	else {	// found
		reducerNodeID_reduceWriteCount_[std::make_pair(nodeId,redID)]++;
	}
}

void Application::notifyIncomingOnDiskMergeWrite(int nodeId, int redID){
	reducerNodeID_reduceWriteCount_[std::make_pair(nodeId,redID)]--;
}

bool Application::datareceivedFromAllMappers(int nodeId, int redID){
	if ((size_t)(reducerNodeID_receivedMapperDataCount_[std::make_pair(nodeId,redID)]) == totalNumberOfMappers_){
		return true;
	}
	return false;
}

int Application::getReduceSpillCount(int nodeId, int redID){
	return reducerNodeID_reduceSpillCount_[std::make_pair(nodeId,redID)];
}

int Application::getReduceWriteCount(int nodeId, int redID){
	return reducerNodeID_reduceWriteCount_[std::make_pair(nodeId,redID)];
}

void Application::adddataWaitingToBeMerged_(int nodeId, size_t dataWaitingToBeMerged, int redID){

	if ( reducerNodeID_dataWaitingToBeMerged_.find(std::make_pair(nodeId,redID)) == reducerNodeID_dataWaitingToBeMerged_.end() ) {	// not found
		reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].chunkSize_.push(dataWaitingToBeMerged);
		reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].numberOfChunksWaitingToBeMerged_ = 1;
	}
	else{	// found
		reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].chunkSize_.push(dataWaitingToBeMerged);
		reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].numberOfChunksWaitingToBeMerged_++;
	}
}

int Application::getNumberOfWaitingTobeMerged(int nodeId, int redID){
	return reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].numberOfChunksWaitingToBeMerged_;
}

size_t Application::getTotalChunkSizeWaitingTobeMerged(int nodeId, int redID){

	size_t mergeSize = 0;

	while(!reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].chunkSize_.empty()){
		mergeSize += reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].chunkSize_.front();
		(reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].chunkSize_).pop();
	}
	return mergeSize;
}

void Application::reduceNumberOfChunksWaitingToBeMerged(int nodeId, int redID){
	reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].numberOfChunksWaitingToBeMerged_-=mapReduceConfig_.getMapreducetaskIoSortFactor();
}

size_t Application::popMergeSize(int nodeId, int redID){

	size_t mergeSize = 0;

	for(int i=0;i<mapReduceConfig_.getMapreducetaskIoSortFactor();i++){
		mergeSize += reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].chunkSize_.front();
		(reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].chunkSize_).pop();
	}
	return mergeSize;
}

size_t Application::popMergeGivenSize(int nodeId, int redID, int mergeCount){

	size_t mergeSize = 0;

	for(int i=0;i<mergeCount;i++){
		mergeSize += reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].chunkSize_.front();
		(reducerNodeID_dataWaitingToBeMerged_[std::make_pair(nodeId,redID)].chunkSize_).pop();
	}
	return mergeSize;
}

void Application::notifyCompletedReducers(){
	numberOfCompletedReducers_++;
}

bool  Application::checkIfAllReducersComplete() const{
	if(numberOfCompletedReducers_ == mapReduceConfig_.getMapreduceJobReduces()){
		return true;
	}
	return false;
}

double Application::getMapFinishTime() const {
	return mapFinishTime_;
}

double Application::getMapStartTime() const {
	return mapStartTime_;
}

double Application::getReduceFinishTime() const {
	return reduceFinishTime_;
}

double Application::getReduceStartTime() const {
	return reduceStartTime_;
}

void Application::setMapFinishTime(double mapFinishTime) {
	mapFinishTime_ = mapFinishTime;
}

void Application::setMapStartTime(double mapStartTime) {
	mapStartTime_ = mapStartTime;
}

void Application::setReduceFinishTime(double reduceFinishTime) {
	reduceFinishTime_ = reduceFinishTime;
}

void Application::setReduceStartTime(double reduceStartTime) {
	reduceStartTime_ = reduceStartTime;
}

void Application::setShuffleFinishTime(double shuffleFinishTime, int redID) {
	shuffleFinishTime_[redID] = shuffleFinishTime;
}

double Application::getShuffleStartTime(int redID){
	return shuffleStartTime_[redID];
}

void Application::setShuffleStartTime(double shuffleStartTime, int redID) {
	shuffleStartTime_[redID] = shuffleStartTime;
}

size_t Application::getWbCompleted() const {
	return wb_completed_;
}

double Application::getMapIntensity() const {
	return mapIntensity_;
}

double Application::getReduceIntensity() const {
	return reduceIntensity_;
}

double Application::getReduceSortIntensity() const {
	return reduceSortIntensity_;
}

void Application::addMapStartTime(int index, double mapStartTime){
	mapStartTimes_[index] = mapStartTime;
}

void Application::addMergeStartTime(int index, double mergeStartTime){
	mergeStartTimes_[index] = mergeStartTime;
}

void Application::addMergeFnStartTime(int index, double mergeFnStartTime){
	if(mergeFnStartTime_.find(index) == mergeFnStartTime_.end() ){	// first time to record
		mergeFnStartTime_[index] = mergeFnStartTime;
	}
}

void Application::addMergeFnFinishTime(int index, double mergeFnFinTime){
	// always record...
	mergeFnFinTime_[index] = mergeFnFinTime;
}

double Application::getMergeFnTime(int index){
	return mergeFnFinTime_[index] - mergeFnStartTime_[index];
}

double Application::getTotalMapMergeTime(int index){
	return mapMergeTime_[index];
}

double Application::getShuffleReadTime(int index){
	return shuffleReadTime_[index] = shuffleReadFinTime_[index] - shuffleReadStartTime_[index];
}

void Application::setShuffleReadStartTime(int index, double time){
	shuffleReadStartTime_[index] = time;
}

void Application::setShuffleReadFinTime(int index, double time){
	shuffleReadFinTime_[index] = time;
}

void Application::addReduceStartTime(int index, double redStartTime){
	redStartTimes_[index] = redStartTime;
}

void Application::addReduceFinishTime(int index, double finishTime){
	totalReduceFinishTime_ += ( finishTime  -  redStartTimes_[index] );
}

double Application::avgReduceTime() const{
	return totalReduceFinishTime_/mapReduceConfig_.getMapreduceJobReduces();
}

void Application::addShuffleStartTime(int index, int redID, double shuffleStartTime){
	shuffleStartTimes_[std::make_pair(index,redID)] = shuffleStartTime;
}

double  Application::getShuffleExperiStartTime(int index, int redID){
	return shuffleStartTimes_[std::make_pair(index,redID)];
}

void Application::addMapFinishTime(int index, double finishTime){
	mapMergeTime_[index] = finishTime  -  mergeStartTimes_[index];
	mergeFnFinTime_[index] = finishTime;

	totalMapFinishTime_ += ( finishTime  -  mapStartTimes_[index] );
}

double Application::avgMapTime() const{
	return totalMapFinishTime_/totalNumberOfMappers_;
}

bool Application::isReducersRequested() const {
	return reducersRequested_;
}

void Application::setReducersRequested(bool reducersRequested) {
	reducersRequested_ = reducersRequested;
}

bool Application::addReadyForOnDiskMergeCount_(int redId){
	if ( readyForOnDiskMergeCount_.find(redId) == readyForOnDiskMergeCount_.end() ) {	// not found
		readyForOnDiskMergeCount_[redId] = 1;
	}
	else{
		readyForOnDiskMergeCount_[redId]++;
	}

	if(readyForOnDiskMergeCount_[redId] > 1){
		return true;
	}
	return false;
}

int Application::subReadyForOnDiskMergeCount_(int redId){
	readyForOnDiskMergeCount_[redId]--;
	return readyForOnDiskMergeCount_[redId];
}

double Application::getTotalShuffleAvgAmongReducers(){
	double sum=0;
	for(int i=0;i<mapReduceConfig_.getMapreduceJobReduces();i++){
		sum += (shuffleFinishTime_[i] - shuffleStartTime_[i]);
	}
	return sum/mapReduceConfig_.getMapreduceJobReduces();
}

bool Application::checkMaxSingleShuffleLimitExceed(size_t resourceSize){
	if(resourceSize > mapReduceConfig_.getMaxSingleShuffleLimit()){
		return true;
	}
	return false;
}

void Application::setReduceRecordCount(int redId, int count){
		redId_recordId_[redId] = count;
}

int Application::getReduceRecordCount(int redId){
		return redId_recordId_[redId];
}

bool Application::areAllReduceRecordComplete(int redId){
	if ( redId_CompletedRecordId_.find(redId) == redId_CompletedRecordId_.end() ) {	// not found
		redId_CompletedRecordId_[redId] = 1;
	}
	else{
		redId_CompletedRecordId_[redId]++;
	}
	if(redId_recordId_[redId] == redId_CompletedRecordId_[redId]){
		return true;
	}
	return false;
}

void Application::setMapRecordCount(int fsID, int count){
	fsId_recordId_[fsID] = count;
}

void Application::setShufflePacketCount(int fsID, int redID, int count){
	fsId_redID_recordId_[std::make_pair(fsID,redID)] = count;
}

int Application::getMapRecordCount(int fsID){
		return fsId_recordId_[fsID];
}

void Application::setMapMergeRecordCount(int fsID, int count){
	fsId_mapMergeRecordId_[fsID] = count;
}

int Application::getMapMergeRecordCount(int fsID){
		return fsId_mapMergeRecordId_[fsID];
}

int Application::getShufflePacketCount(int fsID, int redID){
		return fsId_redID_recordId_[std::make_pair(fsID,redID)];
}

bool Application::areAllMapRecordComplete(int fsId){
	if ( fsId_CompletedRecordId_.find(fsId) == fsId_CompletedRecordId_.end() ) {	// not found
		fsId_CompletedRecordId_[fsId] = 1;
	}
	else{
		fsId_CompletedRecordId_[fsId]++;
	}
	if(fsId_CompletedRecordId_[fsId] == fsId_recordId_[fsId]){
		return true;
	}
	return false;
}

bool Application::areAllShufflePacketComplete(int fsId, int redID){
	if ( fsId_redID_CompletedPacketId_.find(std::make_pair(fsId,redID)) == fsId_redID_CompletedPacketId_.end() ) {	// not found
		fsId_redID_CompletedPacketId_[std::make_pair(fsId,redID)] = 1;
	}
	else{
		fsId_redID_CompletedPacketId_[std::make_pair(fsId,redID)]++;
	}
	if(fsId_redID_CompletedPacketId_[std::make_pair(fsId,redID)] == fsId_redID_recordId_[std::make_pair(fsId,redID)]){
		return true;
	}
	return false;
}

void Application::addMapMergeReadySize(int fsId, size_t size){
	if ( fsId_mapMergeReadysize_.find(fsId) == fsId_mapMergeReadysize_.end() ) {	// not found
		fsId_mapMergeReadysize_[fsId] = size;
	}
	else{
		fsId_mapMergeReadysize_[fsId]+=size;
	}
}

size_t Application::getMapMergeReadySize(int fsId){
	return fsId_mapMergeReadysize_[fsId];
}

recordInfo Application::getRecordInfo(int fsId){
	return fsID_recordInfo_[fsId];
}

int Application::getMapTransferInfo_remainingNumberOfMapRecords(int fsId){
	return fsID_mapTransferInfo_[fsId].remainingNumberOfMapRecords_;
}

int Application::getMergeInfo_remainingNumberOfMapRecords(int fsID, int redID){
	return fsID_mapMergeInfo_[std::make_pair(fsID,redID)].remainingNumberOfMapRecords_;
}

bool Application::getMapTransferInfo_lastRecordExist(int fsId){
	return fsID_mapTransferInfo_[fsId].lastRecordExist_;
}

bool Application::getMergeInfo_lastRecordExist(int fsID, int redID){
	return fsID_mapMergeInfo_[std::make_pair(fsID,redID)].lastRecordExist_;
}

size_t Application::getMapTransferInfo_lastRecordSize(int fsId){
	return fsID_mapTransferInfo_[fsId].lastRecordSize_;
}

double Application::getMergeInfo_lastRecordSize(int fsID, int redID){
	return fsID_mapMergeInfo_[std::make_pair(fsID,redID)].lastRecordSize_;
}

void Application::setMapTransferInfo(int fsId, int remainingNumberOfMapRecords, bool lastRecordExist, size_t lastRecordSize){
	fsID_mapTransferInfo_[fsId].remainingNumberOfMapRecords_ = remainingNumberOfMapRecords;
	fsID_mapTransferInfo_[fsId].lastRecordExist_ = lastRecordExist;
	fsID_mapTransferInfo_[fsId].lastRecordSize_ = lastRecordSize;
}

void Application::setMapMergeInfo(int fsID, int redID, int remainingNumberOfMapRecords, bool lastRecordExist, double lastRecordSize){
	fsID_mapMergeInfo_[std::make_pair(fsID,redID)].remainingNumberOfMapRecords_ = remainingNumberOfMapRecords;
	fsID_mapMergeInfo_[std::make_pair(fsID,redID)].lastRecordExist_ = lastRecordExist;
	fsID_mapMergeInfo_[std::make_pair(fsID,redID)].lastRecordSize_ = lastRecordSize;
}

void Application::decrementmapTransferInfoNumberOfMapRecords(int fsId, int decrementAmount){
	fsID_mapTransferInfo_[fsId].remainingNumberOfMapRecords_-=decrementAmount;
}

void Application::setRecordInfo(int id, size_t eachRecordSize, size_t lastRecordSize, bool lastRecordExist, int remainingRecordCount, bool isMapRecord){

	if(isMapRecord){
		fsID_recordInfo_[id].eachRecordSize_ = eachRecordSize;
		fsID_recordInfo_[id].lastRecordSize_ = lastRecordSize;
		fsID_recordInfo_[id].lastRecordExist_ = lastRecordExist;
		fsID_recordInfo_[id].remainingRecordCount_ = remainingRecordCount;
	}
	else{
		redID_reduceRecordInfo_[id].eachRecordSize_ = eachRecordSize;
		redID_reduceRecordInfo_[id].lastRecordSize_ = lastRecordSize;
		redID_reduceRecordInfo_[id].lastRecordExist_ = lastRecordExist;
		redID_reduceRecordInfo_[id].remainingRecordCount_ = remainingRecordCount;
	}
}

void Application::decrementRecordInfoRemainingMapRecordCount(int fsId){
	fsID_recordInfo_[fsId].remainingRecordCount_--;
}

void Application::decrementRecordInfoRemainingMapRecordCountAmount(int fsId, int amount){
	fsID_recordInfo_[fsId].remainingRecordCount_-=amount;
}

recordInfo Application::getReduceRecordInfo(int redId){
	return redID_reduceRecordInfo_[redId];
}

void Application::decrementRecordInfoRemainingReduceRecordCount(int redId){
	redID_reduceRecordInfo_[redId].remainingRecordCount_--;
}

void Application::decrementRecordInfoRemainingReduceRecordCountAmount(int redId, int amount){
	redID_reduceRecordInfo_[redId].remainingRecordCount_-=amount;
}

bool Application::notifyMapMergeComplete(int fsId){
	if ( fsId_completedMergeCount_.find(fsId) == fsId_completedMergeCount_.end() ) {	// not found
		fsId_completedMergeCount_[fsId] = 1;
	}
	else{
		fsId_completedMergeCount_[fsId]++;
	}
	if(fsId_completedMergeCount_[fsId] == mapReduceConfig_.getMapreduceJobReduces()){
		return true;
	}
	return false;
}

/*returns true if single shuffle limit is exceeded for this fsid and redid*/
bool Application::incShuffleCollectedDataAmount(int fsID, int redID, double shuffleCollectedDataAmount){
	if ( shuffle_collectedDataAmount_.find(std::make_pair(fsID,redID)) == shuffle_collectedDataAmount_.end() ) {	// not found
		shuffle_collectedDataAmount_[std::make_pair(fsID,redID)] = shuffleCollectedDataAmount;
	}
	else{
		shuffle_collectedDataAmount_[std::make_pair(fsID,redID)]+=shuffleCollectedDataAmount;
	}
	if(checkMaxSingleShuffleLimitExceed(shuffle_collectedDataAmount_[std::make_pair(fsID,redID)])){
		return true;
	}
	return false;
}

/*returns true if all the data for this fsid and redid is received from source*/
bool Application::incShuffleFlushedDataAmount(int fsID, int redID){
	if ( shuffle_flushedDataAmount_.find(std::make_pair(fsID,redID)) == shuffle_flushedDataAmount_.end() ) {	// not found
		shuffle_flushedDataAmount_[std::make_pair(fsID,redID)] = shuffle_collectedDataAmount_[std::make_pair(fsID,redID)];
	}
	else{
		shuffle_flushedDataAmount_[std::make_pair(fsID,redID)]+=shuffle_collectedDataAmount_[std::make_pair(fsID,redID)];
	}
	shuffle_collectedDataAmount_[std::make_pair(fsID,redID)] = 0;

	if(shuffle_flushedDataAmount_[std::make_pair(fsID,redID)] == shuffledTotalDataForFsidRedid_[std::make_pair(fsID,redID)]){
		return true;
	}
	return false;
}

void Application::setShuffledTotalDataForFsidRedid(int fsID, int redID, double dataSize){
	shuffledTotalDataForFsidRedid_[std::make_pair(fsID,redID)] = dataSize;
}

int Application::incBufferCompletedPacketCount(int fsId){
	if(  bufferCompletedPacketCount_.find(fsId) == bufferCompletedPacketCount_.end()  ){	// if does not exist create it...
		bufferCompletedPacketCount_[fsId]=0;
	}
	return ++bufferCompletedPacketCount_[fsId];
}

void Application::resetBufferCompletedPacketCount(int fsId){
	bufferCompletedPacketCount_[fsId] = 0;
}

int Application::incMergeBufferCompletedPacketCount(int fsID, int redID){

	if(  bufferMergeCompletedPacketCount_.find(std::make_pair(fsID,redID)) == bufferMergeCompletedPacketCount_.end()  ){	// if does not exist create it...
		bufferMergeCompletedPacketCount_[std::make_pair(fsID,redID)]=0;
	}
	return ++bufferMergeCompletedPacketCount_[std::make_pair(fsID,redID)];
}

int Application::incShuffleWriteDataCount(size_t id){
	if(bufferShuffleWriteDataCount_.find(id) == bufferShuffleWriteDataCount_.end()){	// if does not exist create it...
		bufferShuffleWriteDataCount_[id]=0;
	}
	return ++bufferShuffleWriteDataCount_[id];
}

void Application::resetMergeBufferCompletedPacketCount(int fsID, int redID){
	bufferMergeCompletedPacketCount_[std::make_pair(fsID,redID)] = 0;
}

void Application::resethuffleWriteDataCount(size_t id){
	bufferShuffleWriteDataCount_[id] = 0;
}

int Application::decrementMergeInfoNumberOfMapRecords(int fsID, int redID, int decrementAmount){
	return fsID_mapMergeInfo_[std::make_pair(fsID,redID)].remainingNumberOfMapRecords_-=decrementAmount;
}

int Application::decrementShuffleWriteDataRecords(size_t id, int decrementAmount){
	return reduceWriteid_datasize[id].numPackets_ -= decrementAmount;
}

int Application::getShuffleWriteData_numPackets(size_t id){
	return reduceWriteid_datasize[id].numPackets_;
}

bool Application::getShuffleWriteData_lastRecordExist(size_t id){
	return reduceWriteid_datasize[id].lastRecordExist_;
}

double Application::getShuffleWriteData_lastRecordSize(size_t id){
	return reduceWriteid_datasize[id].lastRecordSize_;
}

void Application::setReducerPartitionSize(int fsID, double sizeofpartition){
	parititionSize_[fsID] = sizeofpartition;
}

double Application::getReducerPartitionSize(int fsID){
	return 	parititionSize_[fsID];
}

void Application::setShuffleWriteDataProperties(size_t id, int numberOfPackets, double totalSplitSize, bool lastRecordExist, double lastRecordSize){
	reduceWriteid_datasize[id].numPackets_ = numberOfPackets;
	reduceWriteid_datasize[id].totalSplitSize_ = totalSplitSize;
}

int Application::getShuffleWriteDataProperties_NumPackets(size_t id){
	return reduceWriteid_datasize[id].numPackets_;
}

double Application::getShuffleWriteDataProperties_DataSize(size_t id){
	return reduceWriteid_datasize[id].totalSplitSize_;
}

bool Application::getShuffleWriteDataProperties_lastRecordExist(size_t id){
	return reduceWriteid_datasize[id].lastRecordExist_;
}

double Application::getShuffleWriteDataProperties_lastRecordSize(size_t id){
	return reduceWriteid_datasize[id].lastRecordSize_;
}

void Application::setShuffleReadInfo(int fsID, int redID, int remainingNumberOfMapRecords, bool lastRecordExist, size_t lastRecordSize){
	fsID_shuffleReadInfo_[std::make_pair(fsID,redID)].remainingNumberOfMapRecords_ = remainingNumberOfMapRecords;
	fsID_shuffleReadInfo_[std::make_pair(fsID,redID)].lastRecordExist_ = lastRecordExist;
	fsID_shuffleReadInfo_[std::make_pair(fsID,redID)].lastRecordSize_ = lastRecordSize;
}

int Application::incShuffleReadBufferCompletedPacketCount(int fsID, int redID){
	if(  bufferShuffleReadCompletedPacketCount_.find(std::make_pair(fsID,redID)) == bufferShuffleReadCompletedPacketCount_.end()  ){	// if does not exist create it...
		bufferShuffleReadCompletedPacketCount_[std::make_pair(fsID,redID)]=0;
	}

	return ++bufferShuffleReadCompletedPacketCount_[std::make_pair(fsID,redID)];
}

int Application::getShuffleReadInfo_remainingNumberOfMapRecords(int fsID, int redID){
	return fsID_shuffleReadInfo_[std::make_pair(fsID,redID)].remainingNumberOfMapRecords_;
}

int Application::decrementShuffleReadNumberOfRecords(int fsID, int redID, int decrementAmount){
	return fsID_shuffleReadInfo_[std::make_pair(fsID,redID)].remainingNumberOfMapRecords_-=decrementAmount;
}

bool Application::getShuffleReadInfo_lastRecordExist(int fsID, int redID){
	return fsID_shuffleReadInfo_[std::make_pair(fsID,redID)].lastRecordExist_;
}

size_t Application::getShuffleReadInfo_lastRecordSize(int fsID, int redID){
	return fsID_shuffleReadInfo_[std::make_pair(fsID,redID)].lastRecordSize_;
}

void Application::resetShuffleReadBufferCompletedPacketCount(int fsID, int redID){
	bufferShuffleReadCompletedPacketCount_[std::make_pair(fsID,redID)] = 0;
}

size_t Application::getRecordSize() const {
	return recordSize_;
}

double Application::getMapSortIntensity() const {
	return mapSortIntensity_;
}
