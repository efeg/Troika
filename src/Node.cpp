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

#include "Node.h"
#include "Switch.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

#define TESTING_ENABLED
#define APP_RESOURCE_AVG_SIZE 10000			// job JAR, configuration, and split information (in bytes)
#define OVERHEAD_DELAY 0.001					// (in second)
#define BUFFER_NUMBER_OF_PACKETS 100			// Each transfer will enqueue BUFFER_NUMBER_OF_PACKETS at once
#define MORE_PACKETS 1

size_t Node::ID_ = 0;
size_t Node::uniqueID_ = 0;
std::map<int, int> nodeExpEventT_switchExpEventT;				// a mapping for expected event types of node and rack to be used for network simulation
std::map<int, int> nodeExpEventT_linkExpEventT;					// a mapping for expected event types of node and their link to be used for network simulation
std::map<int, remainingCapacities> nodeExpEventT_remainingCap;	// up-to-date remaining capacities (per NODE)
std::map<int, enum NodeType> nodeExpEventT_nodeType;			// used to check if is it a RM node

Node::Node(int rackExpectedEventType, enum NodeType nodeType, int expectedEventType, const Cpu cpu, const Harddisk hd, const Memory memory, int outputEventType):
			Module (expectedEventType, NODE),
			nodeID_(ID_++), rackExpectedEventType_(rackExpectedEventType),
			nodeType_(nodeType), cpu_(cpu),
			hd_(hd), memory_(memory),
			outputEventType_(outputEventType){

	nodeExpEventT_switchExpEventT[expectedEventType]=rackExpectedEventType;
	nodeExpEventT_linkExpEventT[expectedEventType]=outputEventType;

	if(g_scheduler.isResourceCalculator()){
		nodeExpEventT_remainingCap[expectedEventType].remainingCpuCapacity=cpu_.getRemainingNumberOfCores();
	}
	if(memory_.getRemainingCapacity() >= (applications.at(0))->getMapReduceConfig().getNodemanagerResourceMemoryMb()*ONE_MB_IN_BYTES){
		nodeExpEventT_remainingCap[expectedEventType].remainingMemoryCapacity=(applications.at(0))->getMapReduceConfig().getNodemanagerResourceMemoryMb()*ONE_MB_IN_BYTES;

		// seize resources for AM for memory
		memory_.setRemainingCapacity((applications.at(0))->getMapReduceConfig().getNodemanagerResourceMemoryMb()*ONE_MB_IN_BYTES);
	}
	else{
		std::cerr<< "Configuration error: Physical memory size is less than node manager resource memory MB" <<std::endl;
		exit(-1);
	}
	nodeExpEventT_nodeType[expectedEventType] = nodeType_;
}

Node::~Node() {
}

size_t Node::getUniqueId(){
	return uniqueID_++;
}

void Node::testing(int numReduces, double eventTime, double avgMapTime, double avgRedTime, double shuffleTime, double mapTime){

	std::ofstream outfile;
	std::stringstream ss;
	ss << "test" << numReduces << ".txt";
	std::string fileName = ss.str();

	outfile.open(fileName, std::ios_base::app);

	if(outfile.fail()){
		std::cerr<< "Outfile Failed!" << std::endl;
		exit(-1);
	}
	else{
		outfile << eventTime << "\n" ;
		outfile << avgMapTime << "\n" ;
		outfile << avgRedTime << "\n" ;
		outfile << shuffleTime << "\n" ;
		outfile << mapTime << "\n" ;
	}
}

bool Node::isUberTask(Event* ev){
	/*
	 * 	In order to run the application as a Uber task, the following should be satisfied:
	 *	mapreduceJobUbertaskEnable: 	True
	 *	mapreduceJobUbertaskMaxmaps:	Threshold for number of maps
	 *	mapreduceJobUbertaskMaxreduces:	Threshold for number of reduces
	 *	mapreduceJobUbertaskMaxbytes:	Threshold for number of input bytes
	 */
	size_t appID = ev->getAppID();

	if ((applications.at(appID))->getMapReduceConfig().isMapreduceJobUbertaskEnable() &&
			(applications.at(appID))->getNumberOfMappers() <= (applications.at(appID))->getMapReduceConfig().getMapreduceJobUbertaskMaxmaps() &&
			(applications.at(appID))->getReduceCount() <= (applications.at(appID))->getMapReduceConfig().getMapreduceJobUbertaskMaxreduces() &&
			(applications.at(appID))->getAppSize() <= (applications.at(appID))->getMapReduceConfig().getMapreduceJobUbertaskMaxbytes()
	){
		return true;
	}
	return false;
}

void Node::recordSetter (size_t partSize, size_t recordSize, int taskID, bool isMapTask, Event* ev){
	size_t appID = ev->getAppID();
	int numberOfRecords = partSize/recordSize;
	size_t eachRecordSize=0;
	bool lastRecordExist = false;
	size_t lastRecordSize;

	if((size_t)numberOfRecords*recordSize < partSize){
		lastRecordSize = partSize - (size_t)numberOfRecords*recordSize;
		lastRecordExist=true;
	}

	double time = ev->getEventTime();
	int eventBehavior;
	enum EntityType entityType;

	if(isMapTask){	// map task
		if(lastRecordExist){
			if(numberOfRecords){	// non-zero
				eachRecordSize = recordSize;
			}
			(applications.at(appID))->setMapRecordCount(ev->getFsId(), numberOfRecords+1);
		}
		else{
			eachRecordSize = partSize/numberOfRecords;
			(applications.at(appID))->setMapRecordCount(ev->getFsId(), numberOfRecords);
		}
		time += (3*YARN_NODEMANAGER_HEARTBEAT_INTERVALMS)/SECOND_IN_MS;
		eventBehavior = READ_NEW_RECORD;
		entityType = MAPTASK;
	}
	else{	// reduce task
		if(lastRecordExist && numberOfRecords){
			eachRecordSize = recordSize;
			(applications.at(appID))->setReduceRecordCount(taskID, numberOfRecords+1);
		}
		else{
			if(numberOfRecords){
				eachRecordSize = partSize/(numberOfRecords);
			}
			if(lastRecordExist){
				(applications.at(appID))->setReduceRecordCount(taskID, numberOfRecords+1);
			}
			else{
				(applications.at(appID))->setReduceRecordCount(taskID, numberOfRecords);
			}
		}
		eventBehavior = READ_REDUCE_RECORDS_TO_SORT;
		entityType = REDUCETASK;
	}

	(applications.at(appID))->setRecordInfo(taskID, eachRecordSize, lastRecordSize, lastRecordExist, numberOfRecords, isMapTask);
	// create a new event for the first record to be read at HD
	Event newEvent(appID, time, -1, -1, ev->getDestEventType(), ev->getDestEventType(),
			eventBehavior, entityType, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), -1, BUFFER_NUMBER_OF_PACKETS);

	eventsList.push(newEvent);
}

void Node::work (Event* ev){
	int currentEvent = ev->getEventBehavior();
	size_t appID = ev->getAppID();

	// check node type
	if(nodeType_ == CLIENT || ((ev->getEntityIns().getEntityType() == APPLICATION ) && ((applications.at(appID))->getClientEventType() == ev->getDestEventType()))){

		// check event behavior
		if(currentEvent == SUBMIT_JOB){	/*DOC: Application run request submitted. "Submit job"*/

			// set application start time
			(applications.at(appID))->setAppStartTime(ev->getEventTime());

			printNodeMessages(CLIENT, currentEvent, appID);
			// Request an application ID from Resource Manager (RM event behavior: GET_APP_ID_RM)
			Event newEvent(appID, ev->getEventTime(), HEARTBEAT_SIZE, 0.0, outputEventType_, (applications.at(appID))->getRmEventType(), GET_APP_ID_RM, APPLICATION);
			eventsList.push(newEvent);
		}
		else if(currentEvent == COPY_APP_RESOURCES){	/*DOC: "Copy application resources"*/

			printNodeMessages(CLIENT, currentEvent, appID);
			// Add delay (Write delay to HDFS!)
			delay(*ev);
		}
		else if(currentEvent == INIT_APPLICATION){

			printNodeMessages(NODEMANAGER, currentEvent, appID);
			// Submit application (RM event behavior: SUBMIT_APP_RM)
			Event newEvent(appID, ev->getEventTime(), HEARTBEAT_SIZE, 0.0, outputEventType_, (applications.at(appID))->getRmEventType(), SUBMIT_APP_RM, APPLICATION);
			eventsList.push(newEvent);

			// Change node type to NODEMANAGER to be able to work with splits...
			nodeType_ = NODEMANAGER;
		}
	}
	else if(nodeType_ == RESOURCEMANAGER){

		if(currentEvent == GET_APP_ID_RM){	/*DOC: "Get Application ID" for Resource Manager*/

			printNodeMessages(RESOURCEMANAGER, currentEvent, appID);

			// Event to copy application resources (split metadata, app jar, configuration) to HDFS (client event behavior: COPY_APP_RESOURCES)
			Event newEvent(appID, (ev->getEventTime() + OVERHEAD_DELAY), HEARTBEAT_SIZE, 0.0, outputEventType_,
					(applications.at(appID))->getClientEventType(), COPY_APP_RESOURCES, APPLICATION);

			eventsList.push(newEvent);
		}
		else if(currentEvent == SUBMIT_APP_RM){	/*DOC: "Submit Application" for Resource Manager*/

			printNodeMessages(RESOURCEMANAGER, currentEvent, appID);

			// Event to start container for a nodemanager (nodemanager event behavior: START_AM_CONTAINER)
			// which NODEMANAGER will be picked? -- Scheduler decides it based on availability of resources.
			int nodemanagerEventType = g_scheduler.schedulerSubmitNewApp(appID, outputEventType_);

			if(nodemanagerEventType != -1){	// successfully scheduled AM
				(applications.at(appID))->setAmEventType(nodemanagerEventType);

				// Event to start container (NODEMANAGER event behavior: START_AM_CONTAINER)
				Event newEvent(appID, (ev->getEventTime()+OVERHEAD_DELAY), HEARTBEAT_SIZE, 0.0, outputEventType_, nodemanagerEventType, START_AM_CONTAINER, OTHER);
				eventsList.push(newEvent);
			}
		}
		else if(currentEvent == ALLOCATE_MAP_RESOURCES_RM){	/*DOC: "Allocate Resources" under Resource Manager (Allocate resources for map tasks) */

			printNodeMessages(RESOURCEMANAGER, currentEvent, appID);
			#ifndef TERMINAL_LOG_DISABLED
						std::cout<< "NODE||INFO: TIME: " << ev->getEventTime() << " FILESPLIT: " << ev->getEntityIns().getAttribute() << std::endl;
			#endif

			// Scheduler works in here...
			// at which node should the mapper work?
			struct waitingTaskInfo mapperInfo = g_scheduler.schedulerGetMapperLocation(appID, ev->getEntityIns().getAttribute(), false,
					outputEventType_, ev->getEntityIns().getAttribute(), ev->getFsId());

			// Mapper Location: mapperInfo.taskLocation (it is -1 in case it is suspended)
			if(mapperInfo.taskLocation == -1){
				#ifndef TERMINAL_LOG_DISABLED
				std::cout<< "NODE||INFO: Suspended Map Task for application: " << appID << std::endl;
				#endif
			}
			else{
				// attribute carries mapperLocation
				Event newEvent(appID, (ev->getEventTime()+OVERHEAD_DELAY),HEARTBEAT_SIZE, 0.0, outputEventType_, (applications.at(appID))->getAmEventType(),
						START_MAP_CONTAINER, OTHER, SEIZETOMASTER, mapperInfo.taskLocation, ev->getEntityIns().getAttribute(), ev->getFsId());
				eventsList.push(newEvent);
			}
		}
		else if(currentEvent == ALLOCATE_REDUCE_RESOURCES_RM){	/*DOC: "Allocate Resources" under Resource Manager (Allocate resources for reduce tasks) */

			printNodeMessages(RESOURCEMANAGER, currentEvent, appID);

			// Scheduler works in here...
			// at which node should the reducer work?
			struct waitingTaskInfo reducerInfo = g_scheduler.schedulerGetReducerLocation(appID, false, outputEventType_);

			// Reducer Location: reducerInfo.taskLocation (it is -1 in case it is suspended)
			if(reducerInfo.taskLocation == -1){
				#ifndef TERMINAL_LOG_DISABLED
				std::cout<< "NODE||INFO: Suspended Reduce Task for application: " << appID << std::endl;
				#endif
			}
			else{
				// Add this reducer location to the list of reducer locations
				(applications.at(appID))->addReducerLocations(reducerInfo.taskLocation);

				// attribute carries reducerLocation
				Event newEvent(appID, (ev->getEventTime()+OVERHEAD_DELAY), HEARTBEAT_SIZE, 0.0, outputEventType_, (applications.at(appID))->getAmEventType(),
						START_REDUCE_CONTAINER, OTHER, SEIZETOMASTER, reducerInfo.taskLocation);

				eventsList.push(newEvent);
			}
		}
		else if(currentEvent == RELEASE_REMOTE_FS_READ_RES){	/*DOC: release seized HD read resources in remote node*/
			// release seized HD resources in remote node
			if(ev->getRecordId() == MORE_PACKETS){	// there are more packets that were not able to fit in buffer
				// release
				hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

				if ((applications.at(appID))->incBufferCompletedPacketCount(ev->getFsId()) == BUFFER_NUMBER_OF_PACKETS){

					// update the remaining number of transfer records
					(applications.at(appID))->decrementmapTransferInfoNumberOfMapRecords(ev->getFsId(),BUFFER_NUMBER_OF_PACKETS);

					// get the remaining number of transfer records
					int remaining = (applications.at(appID))->getMapTransferInfo_remainingNumberOfMapRecords(ev->getFsId());

					ev->setEventBehavior(RECEIVE_FS_TRANSFER_REQUEST);
					ev->setSeizedResQuantity(0);

					int recordCount = BUFFER_NUMBER_OF_PACKETS;
					if(remaining <= BUFFER_NUMBER_OF_PACKETS){	// it is the last set
						recordCount = remaining;
						ev->setRecordId(-1);
					}

					for(int i=0;i<recordCount;i++){
						// read filesplit from remote and send a response back to the origin
						// send each record separately.

						hd_.work(HD_READ_FROM_REMOTE, (applications.at(appID))->getRecordSize(), ev, outputEventType_);
						ev->incGlobalEntityId();
					}

					if(remaining <= BUFFER_NUMBER_OF_PACKETS && (applications.at(appID))->getMapTransferInfo_lastRecordExist(ev->getFsId())){	// last record of last set
						hd_.work(HD_READ_FROM_REMOTE, (applications.at(appID))->getMapTransferInfo_lastRecordSize(ev->getFsId()), ev, outputEventType_);
					}

					// reset bufferCompletedPacketCount
					(applications.at(appID))->resetBufferCompletedPacketCount(ev->getFsId());
				}
			}
			else{	// the last set of input. just release...
				hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			}
		}
		else if(currentEvent == RECEIVE_FS_TRANSFER_REQUEST){	/*DOC: transfer request is received for a file split*/
			// read filesplit from remote and send read data back to the origin
			// read data from the local disk
			// "read shuffled output" in reducer for each record feed reduce function
			size_t totalSplitSize = (applications.at(appID))->getFsSize(ev->getFsId());
			int numberOfMapRecords = totalSplitSize/((applications.at(appID))->getRecordSize());

			bool lastRecordExist = false;
			size_t lastRecordSize=0;
			if((size_t)numberOfMapRecords*((applications.at(appID))->getRecordSize()) < totalSplitSize){
				lastRecordSize = totalSplitSize - (size_t)numberOfMapRecords*((applications.at(appID))->getRecordSize());
				lastRecordExist=true;
			}
			if(lastRecordExist){
				(applications.at(appID))->setMapRecordCount(ev->getFsId(), numberOfMapRecords+1);
			}
			else{
				(applications.at(appID))->setMapRecordCount(ev->getFsId(), numberOfMapRecords);
			}
			if(numberOfMapRecords > BUFFER_NUMBER_OF_PACKETS){	// enqueue BUFFER_NUMBER_OF_PACKETS packets to the NIC queue

				// set recordId which will be used to determine if there is a need to send more
				// packets after the releasing resources of BUFFER_NUMBER_OF_PACKETSth packet
				ev->setRecordId(MORE_PACKETS);

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					// read filesplit from remote and send a response back to the origin
					// send each record separately.
					hd_.work(HD_READ_FROM_REMOTE, (applications.at(appID))->getRecordSize(), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				// save remaining number of packets to send and the information whether a last packet exists and the lastpacketsize
				(applications.at(appID))->setMapTransferInfo(ev->getFsId(), numberOfMapRecords,lastRecordExist,lastRecordSize);
			}
			else{	// send all packets to the NIC queue

				for(int i=0;i<numberOfMapRecords;i++){
					// read filesplit from remote and send a response back to the origin
					// send each record separately.
					hd_.work(HD_READ_FROM_REMOTE, (applications.at(appID))->getRecordSize(), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				if(lastRecordExist){
					hd_.work(HD_READ_FROM_REMOTE, lastRecordSize, ev, outputEventType_);
				}
			}
		}
		else if(currentEvent == NODE_HD_CFQ_EVENT){
			hd_.work(HD_CFQ_EVENT, ev->getSeizedResQuantity(), ev, outputEventType_);
		}
	}
	else if(nodeType_ == NODEMANAGER){

		if(currentEvent == START_AM_CONTAINER){	/*DOC: eventBehavior: "Start Container" under Node Manager*/

			printNodeMessages(NODEMANAGER, currentEvent, appID);

			// required resources for AM
			size_t amResourcesMB = (applications.at(appID))->getAmResourceMb();
			if(g_scheduler.isResourceCalculator()){
				int amCpuVcores = (applications.at(appID))->getamCpuVcores();
				// seize resources for AM for CPU
				cpu_.setRemainingNumberOfCores(cpu_.getRemainingNumberOfCores() - amCpuVcores);
			}
			// seize resources for AM for memory
			memory_.setRemainingCapacity(memory_.getRemainingCapacity() - (amResourcesMB*ONE_MB_IN_BYTES));

			// Event to launch application master's process (NODEMANAGER event behavior: LAUNCH_APP_MASTER)
			Event newEvent(appID, ev->getEventTime()+OVERHEAD_DELAY, 0.0, 0.0, ev->getDestEventType(), ev->getDestEventType(), LAUNCH_APP_MASTER, OTHER);
			eventsList.push(newEvent);
		}
		else if(currentEvent == LAUNCH_APP_MASTER){	/*DOC: "Launch Application Master's Process" under Node Manager*/

			printNodeMessages(NODEMANAGER, currentEvent, appID);

			// Event to Initialize Application (NODEMANAGER event behavior: INIT_APPLICATION)
			Event newEvent(appID, ev->getEventTime()+OVERHEAD_DELAY, 0.0, 0.0, ev->getDestEventType(), ev->getDestEventType(), INIT_APPLICATION, MAPTASK);
			eventsList.push(newEvent);
		}
		else if(currentEvent == INIT_APPLICATION){	/*DOC: "Initialize Application" under Node Manager*/

			printNodeMessages(NODEMANAGER, currentEvent, appID);

			// Event to Retrieve Input Splits (NODEMANAGER event behavior: RETRIEVE_SPLITS)
			Event newEvent(appID, ev->getEventTime()+OVERHEAD_DELAY, HEARTBEAT_SIZE, 0.0, ev->getDestEventType(), ev->getDestEventType(), RETRIEVE_SPLITS, MAPTASK);
			eventsList.push(newEvent);
		}
		else if(currentEvent == RETRIEVE_SPLITS){	/*DOC: "Retrieve Input Splits" under Node Manager*/

			printNodeMessages(NODEMANAGER, currentEvent, appID);
			delay(*ev);
		}
		else if(currentEvent == START_MAP_CONTAINER){	/*DOC: AM "Start Container" for map task*/

			printNodeMessages(NODEMANAGER, currentEvent, appID);

			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: MAPPER__________________: " << ev->getEntityIns().getAttribute() << std::endl;
			#endif

			(applications.at(appID))->addMapStartTime(ev->getFsId(), ev->getEventTime());

			// Event to Launch (NODEMANAGER event behavior: LAUNCH) - attribute LAUNCH_MAP represents this

			if(ev->getDestEventType() != ev->getEntityIns().getAttribute()){	// launch at non local
				Event newEvent(appID, ev->getEventTime(), HEARTBEAT_SIZE, 0.0, outputEventType_, ev->getEntityIns().getAttribute(), LAUNCH,
						MAPTASK, SEIZETOMASTER, LAUNCH_MAP, ev->getFsLoc(), ev->getFsId());

				eventsList.push(newEvent);
			}
			else{	// launch at local
				Event newEvent(appID, ev->getEventTime(), HEARTBEAT_SIZE, 0.0, ev->getEntityIns().getAttribute(), ev->getEntityIns().getAttribute(),
						LAUNCH, MAPTASK, SEIZETOMASTER, LAUNCH_MAP, ev->getFsLoc(), ev->getFsId());

				eventsList.push(newEvent);
			}
		}
		else if(currentEvent == START_REDUCE_CONTAINER){	/*DOC: AM "Start Container" for reducer*/

			printNodeMessages(NODEMANAGER, currentEvent, appID);

			// follow AM controlled reducers
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: REDUCER__________________: " << ev->getEntityIns().getAttribute() << std::endl;
			#endif

			// Event to Launch (NODEMANAGER event behavior: LAUNCH) - attribute LAUNCH_REDUCE represents this
			if(ev->getDestEventType() != ev->getEntityIns().getAttribute()){	// launch at non local
				Event newEvent(appID, ev->getEventTime(), HEARTBEAT_SIZE, 0.0, outputEventType_, ev->getEntityIns().getAttribute(),
						LAUNCH, MAPTASK, SEIZETOMASTER, LAUNCH_REDUCE);

				eventsList.push(newEvent);
			}
			else{	// launch at local
				Event newEvent(appID, ev->getEventTime(), HEARTBEAT_SIZE, 0.0, ev->getEntityIns().getAttribute(), ev->getEntityIns().getAttribute(),
						LAUNCH, MAPTASK, SEIZETOMASTER, LAUNCH_REDUCE);

				eventsList.push(newEvent);
			}
		}
		else if(currentEvent == LAUNCH){	/*DOC: "Launch"*/

			printNodeMessages(NODEMANAGER, currentEvent, appID);
			// Launch delay

			// Event to Retrieve job resources (NODEMANAGER event behavior: RETRIEVE_JOB_RESOURCES)
			Event newEvent(appID, (ev->getEventTime() + OVERHEAD_DELAY), 0.0, 0.0, ev->getDestEventType(), ev->getDestEventType(),
					RETRIEVE_JOB_RESOURCES, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId());

			eventsList.push(newEvent);
		}
		else if(currentEvent == RETRIEVE_JOB_RESOURCES){	/*DOC: "Retrieve job resources"*/

			printNodeMessages(NODEMANAGER, currentEvent, appID);

			// Note that read and (if required) transfer of filesplit and related delays occurs "after" this step.
			// Event to Run Task (NODEMANAGER event behavior: RUN_TASK)
			Event newEvent(appID, ev->getEventTime(), 0.0, 0.0, ev->getDestEventType(), ev->getDestEventType(), RUN_TASK,
					MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId());

			eventsList.push(newEvent);
		}
		else if(currentEvent == RUN_TASK){	/*DOC: eventBehavior: "Run task"*/

			printNodeMessages(NODEMANAGER, currentEvent, appID);

			// Event to Run Task (map or reduce)

			// is it a map task (attribute LAUNCH_MAP: map task)
			if(ev->getEntityIns().getAttribute() == LAUNCH_MAP){

				if( (applications.at(appID))->getMapStartTime() < 0 ){
					(applications.at(appID))->setMapStartTime(ev->getEventTime());
				}
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "NODE||INFO: AppID: " << appID <<  " >MAPPER ID: " << ev->getFsId() << " START TIME: " << ev->getEventTime() << std::endl;
				#endif

				// increase active map task count
				g_scheduler.incrementActiveTaskCount(appID, true);

				(applications.at(appID))->addMapStartTime(ev->getFsId(), ev->getEventTime());

				// seize resources for map task for memory and CPU
				if(g_scheduler.isResourceCalculator()){
					cpu_.setRemainingNumberOfCores(cpu_.getRemainingNumberOfCores() - (applications.at(appID))->getSeizedMapCpuVcores());
				}
				memory_.setRemainingCapacity(memory_.getRemainingCapacity() - (applications.at(appID))->getSeizedMapreduceMapMemory());

				/*
				 * read the input split (is it local, is it in the same rack or at another rack)
				 * simulate copying data which is saved in HDFS to its local HD
				 * if data is not in local, data request is sent to the worker node that has the data.
				 * when data transfer is complete, the process continues...
				*/
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "NODE||INFO: STARTED READING FS: " << ev->getFsId() << " at "<< ev->getEventTime() << " located in node: " << ev->getFsLoc() << std::endl;
				#endif

				// local (read)
				if(ev->getFsLoc() == ev->getDestEventType()){
					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "NODE||INFO: READ LOCAL " << ev->getFsId() << std::endl;
					#endif
					// read data from the local disk
					// "read shuffled output" in reducer for each record feed reduce function
					size_t totalSplitSize = (applications.at(appID))->getFsSize(ev->getFsId());

					recordSetter (totalSplitSize, (applications.at(appID))->getRecordSize(), ev->getFsId(), true, ev);
				}
				// same rack (transfer & read)
				else if(nodeExpEventT_switchExpEventT[ev->getFsLoc()] == nodeExpEventT_switchExpEventT[ev->getDestEventType()]){
					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "NODE||INFO: READ SAME RACK " << ev->getFsId()  << std::endl;
					#endif
					// send transfer request to file split location (NODEMANAGER event behavior: RECEIVE_FS_TRANSFER_REQUEST)
					// save return address in "attribute"
					Event newEvent(appID, ev->getEventTime() + (3*YARN_NODEMANAGER_HEARTBEAT_INTERVALMS)/SECOND_IN_MS, HEARTBEAT_SIZE, 0.0, outputEventType_, ev->getFsLoc(),
							RECEIVE_FS_TRANSFER_REQUEST, MAPTASK, SEIZETOMASTER, ev->getDestEventType(), ev->getFsLoc(), ev->getFsId());

					eventsList.push(newEvent);
				}
				// somewhere in the cluster (transfer & read)
				else{
					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "NODE||INFO: READ ANOTHER RACK " << ev->getFsId()  << std::endl;
					#endif
					// send transfer request to file split location (NODEMANAGER event behavior: RECEIVE_FS_TRANSFER_REQUEST)
					// save return address in "attribute"
					Event newEvent(appID, ev->getEventTime() + (3*YARN_NODEMANAGER_HEARTBEAT_INTERVALMS)/SECOND_IN_MS, HEARTBEAT_SIZE, 0.0, outputEventType_, ev->getFsLoc(),
							RECEIVE_FS_TRANSFER_REQUEST, MAPTASK, SEIZETOMASTER, ev->getDestEventType(), ev->getFsLoc(), ev->getFsId());

					eventsList.push(newEvent);
				}
			}
			// is it a reducer (attribute LAUNCH_REDUCE: reduce task)
			else if(ev->getEntityIns().getAttribute() == LAUNCH_REDUCE){

				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "NODE||INFO: AppID: " << appID <<  " >REDUCER START TIME: "<< ev->getEventTime() << std::endl;
				#endif

				// increase active reduce task count
				g_scheduler.incrementActiveTaskCount(appID, false);


				// seize resources for Reducer for memory and CPU
				if(g_scheduler.isResourceCalculator()){
					cpu_.setRemainingNumberOfCores(cpu_.getRemainingNumberOfCores() - (applications.at(appID))->getSeizedReduceCpuVcores());
				}
				memory_.setRemainingCapacity(memory_.getRemainingCapacity() - (applications.at(appID))->getSeizedMapreduceReduceMemory());
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "NODE||INFO: REDUCER______________------------- " <<  ev->getDestEventType() << std::endl;
				#endif

				if((applications.at(appID))->isReducerCompletionWaitersHasElement()){
					// then there are some map output waiting to be shuffled to reducers

					size_t totalwaiters = (applications.at(appID))->gettotalReducerCompletionWaiters();
					for(size_t i=0; i < totalwaiters;i++){
						#ifndef TERMINAL_LOG_DISABLED
						std::cout << "NODE||INFO: SHUFFLE ALL"  <<  std::endl;
						#endif
						reducerWaiterData waiterData = (applications.at(appID))->getReducerCompletionWaiter(i);
						int finalDest = (applications.at(appID))->getReducerLocation(waiterData.attribute_);

						if(finalDest != -1){	// reducer location found (which means reduce task was created) and data was not shuffled (since it was waiting in waiterData)

							// Shuffle start time
							if((applications.at(appID))->getShuffleStartTime(waiterData.attribute_) < 0){
								(applications.at(appID))->setShuffleStartTime(ev->getEventTime(), waiterData.attribute_);
							}
							// add shuffle start time
							(applications.at(appID))->addShuffleStartTime(waiterData.fsID_, waiterData.attribute_, ev->getEventTime());
							// read data at corresponding node to be sent to a reducer (waiterData.attribute_ will be used to get reducer location)
							Event newEvent(appID, ev->getEventTime(), waiterData.neededResQuantity_, 0, waiterData.myLoc_, waiterData.myLoc_,
									SHUFFLE_READ_DATA, REDUCETASK, SEIZETOMASTER, waiterData.attribute_, -1, waiterData.fsID_, waiterData.attribute_);
							eventsList.push(newEvent);

							// data shuffle started so set shuffle flag to true
							(applications.at(appID))->signalReducerCompletionWaiter(i);
						}
					}
					// clear shuffle started elements from reducer completion waiter list (those who wait from reducer to be created to start shuffle...)
					(applications.at(appID))->clearFinishedReducerCompletionWaiters();
				}
			}
			else{
				std::cerr << "Unrecognized task type: " << appID << std::endl;
			}
		}

		else if(currentEvent == READ_NEW_RECORD){		// read one more record for map function

			recordInfo newRecord = (applications.at(appID))->getRecordInfo(ev->getFsId());
			if(newRecord.remainingRecordCount_ > BUFFER_NUMBER_OF_PACKETS){	// send BUFFER_NUMBER_OF_PACKETS now. upon completion send more...

				// decrement remaining map record count
				(applications.at(appID))->decRecordInfoRemainingMapRecordCount(ev->getFsId(), BUFFER_NUMBER_OF_PACKETS);

				ev->setRecordId(newRecord.remainingRecordCount_);
				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					// read each record and upon completion release the seized resource and start map function on the record just read
					hd_.work(HD_READ_FOR_MAP_TASK, newRecord.eachRecordSize_, ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
			else{	// send all the remaining
				// reset remaining map record count
				(applications.at(appID))->decRecordInfoRemainingMapRecordCount(ev->getFsId(), newRecord.remainingRecordCount_);

				ev->setRecordId(0);

				for(int i=0;i<newRecord.remainingRecordCount_;i++){
					hd_.work(HD_READ_FOR_MAP_TASK, newRecord.eachRecordSize_, ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				if(newRecord.lastRecordExist_){
					hd_.work(HD_READ_FOR_MAP_TASK, newRecord.lastRecordSize_, ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
			// if there is no other record to read for this filesplit do nothing...
		}

		else if(currentEvent == FS_RECORD_READ_FINISH){	/*DOC: completion of filesplit record read in the map. now the map is ready to start map function and corresponding hd resource will be released */
			// release disk resource
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			// done reading! continue with next step (map function).
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: FS SIZE: " <<  ev->getNeededResQuantity()<< " fsID " << ev->getFsId() <<  std::endl; // process this data in mapper cpu
			#endif
			// At this point check if map function is "unit" function... in case of unit function, transient data was never spilled to disk other at the end of the map.

			Event newEvent(appID, ev->getEventTime(), ev->getNeededResQuantity(), ev->getSeizedResQuantity(), ev->getNextEventType(), ev->getDestEventType(),
					RUN_MAP_FUNCTION, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRecordId());

			eventsList.push(newEvent);
		}
		else if(currentEvent == RELEASE_REMOTE_FS_READ_RES){	/*DOC: release seized hd read resources in remote node*/
			// release seized hd resources in remote node
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			if(ev->getRecordId() == 1 &&
					(applications.at(appID))->incBufferCompletedPacketCount(ev->getFsId()) == BUFFER_NUMBER_OF_PACKETS
					){	// there are more packets that were not able to fit in buffer

				// update the remaining number of transfer records
				(applications.at(appID))->decrementmapTransferInfoNumberOfMapRecords(ev->getFsId(),BUFFER_NUMBER_OF_PACKETS);

				// get the remaning number of transfer records
				int remaining = (applications.at(appID))->getMapTransferInfo_remainingNumberOfMapRecords(ev->getFsId());

				if(remaining > BUFFER_NUMBER_OF_PACKETS){	// not the last set

					ev->setEventBehavior(RECEIVE_FS_TRANSFER_REQUEST);
					ev->setSeizedResQuantity(0);

					for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
						// read filesplit from remote and send a response back to the origin

						// send each record separately.
						hd_.work(HD_READ_FROM_REMOTE, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
						ev->incGlobalEntityId();
					}
				}
				else{	// it is the last set
					// last set of records!
					ev->setRecordId(-1);
					ev->setEventBehavior(RECEIVE_FS_TRANSFER_REQUEST);
					ev->setSeizedResQuantity(0);
					for(int i=0;i<remaining;i++){
						// read filesplit from remote and send a response back to the origin
						// send each record separately.

						hd_.work(HD_READ_FROM_REMOTE, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
						ev->incGlobalEntityId();
					}
					if((applications.at(appID))->getMapTransferInfo_lastRecordExist(ev->getFsId())){
						hd_.work(HD_READ_FROM_REMOTE, (applications.at(appID))->getMapTransferInfo_lastRecordSize(ev->getFsId()), ev, outputEventType_);
					}
				}
				// reset bufferCompletedPacketCount
				(applications.at(appID))->resetBufferCompletedPacketCount(ev->getFsId());
			}
		}

		else if(currentEvent == RECEIVE_FS_TRANSFER_REQUEST){	/*DOC: transfer request is received for a file split*/
			// read filesplit from remote and send read data back to the origin
			// read data from the local disk
			// "read shuffled output" in reducer for each record feed reduce function
			size_t totalSplitSize = (applications.at(appID))->getFsSize(ev->getFsId());
			int numberOfMapRecords = totalSplitSize/((applications.at(appID))->getRecordSize());
			bool lastRecordExist = false;
			size_t lastRecordSize=0;
			if((size_t)numberOfMapRecords*((applications.at(appID))->getRecordSize()) < totalSplitSize){
				lastRecordSize = totalSplitSize - (size_t)numberOfMapRecords*((applications.at(appID))->getRecordSize());
				lastRecordExist=true;
			}
			if(lastRecordExist){
				(applications.at(appID))->setMapRecordCount(ev->getFsId(), numberOfMapRecords+1);
			}
			else{
				(applications.at(appID))->setMapRecordCount(ev->getFsId(), numberOfMapRecords);
			}
			if(numberOfMapRecords > BUFFER_NUMBER_OF_PACKETS){	// enqueue BUFFER_NUMBER_OF_PACKETS packets to the NIC queue

				// set recordId which will be used to determine if there is a need to send more
				// packets affter the releasing resources of BUFFER_NUMBER_OF_PACKETSth packet
				ev->setRecordId(1);

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					// read filesplit from remote and send a response back to the origin
					// send each record separately.
					hd_.work(HD_READ_FROM_REMOTE, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				// save remaining number of packets to send and the information whether a last packet exists and the lastpacketsize
				(applications.at(appID))->setMapTransferInfo(ev->getFsId(), numberOfMapRecords,lastRecordExist,lastRecordSize);
			}
			else{	// send all packets to the NIC queue

				for(int i=0;i<numberOfMapRecords;i++){
					// read filesplit from remote and send a response back to the origin
					// send each record separately.
					hd_.work(HD_READ_FROM_REMOTE, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				if(lastRecordExist){
					hd_.work(HD_READ_FROM_REMOTE, lastRecordSize, ev, outputEventType_);
				}
			}
		}
		else if(currentEvent == WRITE_TRANSFERRED_FS_TO_LOCAL){	/*DOC: transfer from remote node is complete. now write the transferred filesplit*/
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: RECEIVED FS: " << ev->getFsId() << " at "<< ev->getEventTime() << " located in node: " << ev->getFsLoc() << std::endl;
			#endif

			// write transferred data to the local disk
			hd_.work(HD_WRITE_TO_LOCAL, ev->getNeededResQuantity(), ev, outputEventType_);
		}
		else if(currentEvent == RELEASE_FS_TRANSFERRED_WRITE_RES){	/*DOC: release seized hd write resources in local node*/
			// release disk resource and check waiting queue for other write requests for mapper
			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			// read transferred data from the local disk
			// create a new event for last record to be read at HD
			Event newEvent(appID, ev->getEventTime(), ev->getNeededResQuantity(), -1, ev->getDestEventType(), ev->getDestEventType(),
					-1, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), -1);

			// read each record and upon completion release the seized resource and start map function on the record just read

			hd_.work(HD_READ_FOR_MAP_TASK, ev->getNeededResQuantity(), &newEvent, outputEventType_);
		}
		else if(currentEvent == RUN_MAP_FUNCTION){	/*DOC: Map function */
			// execute map function

			// note that in any case seized_mapCpuVcores will be fetched (even if the default resource calculator is used)
			int seized_mapCpuVcores= (applications.at(appID))->getSeizedMapCpuVcores();
			// calculate spillLimitInBytes
			double sortSpillPercent = (applications.at(appID))->getMapReduceConfig().getMapreduceMapSortSpillPercent();
			size_t mapreduceTaskIOSortMb = (applications.at(appID))->getMapReduceConfig().getMapreduceTaskIoSortMb();
			size_t spillLimitInBytes = sortSpillPercent * (mapreduceTaskIOSortMb  << 20);
			double effectiveCoreCountPerMapTask;

			if (activeMappersInThisNode_.find(std::make_pair(ev->getFsId(),ev->getAppID())) == activeMappersInThisNode_.end()){	// not found
				activeMappersInThisNode_[std::make_pair(ev->getFsId(),ev->getAppID())] = '1';
			}
			int mapCapacityOfThisNode = g_scheduler.getnodeId_maxNumberOfMapTaskPerNode(ev->getDestEventType());
			int freeSlotsInThisNode = mapCapacityOfThisNode - activeMappersInThisNode_.size();
			int remainingNumberOfMapTasks = (applications.at(appID))->getTotalNumberOfMappers() - (applications.at(appID))->getCompletedMapperCount();

			if(remainingNumberOfMapTasks  < freeSlotsInThisNode){
				effectiveCoreCountPerMapTask = (freeSlotsInThisNode/(double)remainingNumberOfMapTasks)*cpu_.getRemainingNumberOfCores()/((double)mapCapacityOfThisNode);
			}
			else{
				effectiveCoreCountPerMapTask = cpu_.getRemainingNumberOfCores()/(double)mapCapacityOfThisNode;
			}
			if (seized_mapCpuVcores < effectiveCoreCountPerMapTask){
				effectiveCoreCountPerMapTask = seized_mapCpuVcores;
			}

			cpu_.setEffectiveMapCoreCapacity(effectiveCoreCountPerMapTask);

			// spills will create a map sort event (Event: RUN_MAP_SORT)
			cpu_.mapFunction(ev, spillLimitInBytes, (mapreduceTaskIOSortMb  << 20), seized_mapCpuVcores, (applications.at(appID))->getMapIntensity(),
					(applications.at(appID))->getMapRecordCount(ev->getFsId()));
		}
		// partition sort...
		else if(currentEvent == RUN_MAP_SORT){	/*DOC: map sort function */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: SORT & PARTITION_____TIME:   " << ev->getEventTime() << " fsid: " << ev->getFsId() << " needed " << ev->getNeededResQuantity() << " RedID" << ev->getRedId() <<std::endl;
			#endif

			if(ev->getRedId() == NON_SPILL){	// NON_SPILL
				cpu_.nonSpillComplete(ev->getFsId(), ev->getNeededResQuantity(), ev);
			}
			else{	// SPILL

				if( (applications.at(appID))->isThereACombiner() ){	// there is a combiner function
					cpu_.sortWork(ev, (applications.at(appID))->getMapIntensity(), true);
				}
				else{
					// set recordID to MAP_SORT_SOURCE to signal that the data flow is coming from a map sort
					ev->setRecordId(MAP_SORT_SOURCE);
					cpu_.sortWork(ev, (applications.at(appID))->getMapIntensity());
				}
			}
		}
		else if(currentEvent == RUN_COMBINER){	/*DOC: combiner */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: COMBINER_____TIME:   " << ev->getEventTime() << " ev->getNeededResQuantity() " << ev->getNeededResQuantity() << " fsID " << ev->getFsId() << std::endl;
			#endif
			cpu_.decCpuUserCount(ev->getFsId(), ev->getAppID());
			// set recordID to COMBINER_SOURCE to signal that the data flow is coming from a combiner
			ev->setRecordId(COMBINER_SOURCE);
			// set the spill amount so that when the write to disk is completed, it can be cleared from the buffer of map task
			ev->setSpillTally(ev->getNeededResQuantity());
			double compressionRatio = (applications.at(appID))->getMapOutputVolume() * 0.01;
			cpu_.combinerWork(ev->getNeededResQuantity()*compressionRatio, ev->getEntityIns().getAttribute(), ev,
					(applications.at(appID))->getCombinerIntensity());
		}
		// spill...
		else if(currentEvent == RUN_MAP_SPILL){	/*DOC: map spill function */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: SPILL_____TIME:   " << ev->getEventTime() << " ev->getNeededResQuantity() " << ev->getNeededResQuantity() << " fsID " << ev->getFsId() << std::endl;
			#endif

			cpu_.decCpuUserCount(ev->getFsId(), ev->getAppID());
			// Now do the spill work...
			// write data to the local disk (spill)

			if( ev->getRecordId() == COMBINER_SOURCE){	// there was a combiner (combiner related compression is applied)
				hd_.work(HD_WRITE_TRANSFERRED_TO_LOCAL, ev->getNeededResQuantity()* ((applications.at(appID))->getCombinerCompressionPercent() * 0.01), ev, outputEventType_);
			}
			else{	// no combiner (map output compression is applied)
				// set the spill amount so that when the write to disk is completed, it can be cleared from the buffer of map task
				ev->setSpillTally(ev->getNeededResQuantity());
				hd_.work(HD_WRITE_TRANSFERRED_TO_LOCAL, ev->getNeededResQuantity()* ((applications.at(appID))->getMapOutputVolume() * 0.01), ev, outputEventType_);
			}
		}
		else if(currentEvent == MAP_REMAINING_WORK){	/*DOC: Map (check CPU suspend requirement)*/
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: CPU SUSPEND CHECK_____TIME:   " << ev->getEventTime() << "FsId: " << ev->getFsId() << std::endl;
			#endif
			double sortSpillPercent = (applications.at(appID))->getMapReduceConfig().getMapreduceMapSortSpillPercent();

			cpu_.remainingMapWork(ev->getNeededResQuantity(), ev->getEntityIns().getAttribute(), ev, ev->getSeizedResQuantity(), (size_t)(ev->getSeizedResQuantity() * sortSpillPercent),
					(applications.at(appID))->getMapIntensity() );
		}
		else if(currentEvent == MAP_MERGE_READY){	/*DOC: mapper (merge ready) preceded by map spill (RUN_MAP_SPILL)*/
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: MERGE READY_____TIME:   " << ev->getEventTime() << " appID " << appID <<  " fsID " << ev->getFsId() << " seized: "<< ev->getSeizedResQuantity() << std::endl;
			#endif

			// release disk resource for spill write
			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			// notify spill completion and initiate other spills if needed
			bool allSpillsCompleted = cpu_.mapSpillCompleted(ev);
			// add map merge ready size
			(applications.at(appID))->addMapMergeReadySize(ev->getFsId(), ev->getNeededResQuantity());

			if(allSpillsCompleted){

				size_t mapperOutputSize = (applications.at(appID))->getMapMergeReadySize(ev->getFsId());
				(applications.at(appID))->addMergeStartTime(ev->getFsId(), ev->getEventTime());
				#ifndef TERMINAL_LOG_DISABLED
				std::cout<< "NODE||INFO: START MERGING FsID: "<< ev->getFsId() << " in Mapper for application: " << appID << " mapperOutputSize " << mapperOutputSize <<  " time: " << ev->getEventTime() << std::endl;
				#endif
				int totalReducerCount = (applications.at(appID))->getReduceCount();
				double parititonOfEachReducer = mapperOutputSize/(double)totalReducerCount;

				ev->setSeizedResQuantity(0);
				ev->setNextEventType(ev->getDestEventType());

				int numberOfMergeRecords = parititonOfEachReducer/((applications.at(appID))->getRecordSize());
				(applications.at(appID))->setReducerPartitionSize(ev->getFsId(), parititonOfEachReducer);

				bool lastRecordExist = false;
				double lastRecordSize=0;
				if((size_t)numberOfMergeRecords*((applications.at(appID))->getRecordSize()) < parititonOfEachReducer){
					lastRecordSize = parititonOfEachReducer - (size_t)numberOfMergeRecords*((applications.at(appID))->getRecordSize());
					lastRecordExist=true;
				}

				if(lastRecordExist){
					(applications.at(appID))->setMapMergeRecordCount(ev->getFsId(), numberOfMergeRecords+1);
				}
				else{
					(applications.at(appID))->setMapMergeRecordCount(ev->getFsId(), numberOfMergeRecords);
				}

				if(numberOfMergeRecords > BUFFER_NUMBER_OF_PACKETS){
					ev->setRecordId(MORE_PACKETS);
				}
				else{
					ev->setRecordId(-1);
				}
				for(int i=0;i<totalReducerCount;i++)
				{
					// use redID to determine the merge count
					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "NODE||INFO: Start Merge: " << ev->getFsId() << " totalReducerCount " << totalReducerCount<<  " time: " << ev->getEventTime() << " mapperOutputSize: " << mapperOutputSize/totalReducerCount << " redId " << i << std::endl;
					#endif
					// merge partitions for each reducer
					ev->setRedId(i);

					// but read each record separately
					if(numberOfMergeRecords > BUFFER_NUMBER_OF_PACKETS){	// enqueue BUFFER_NUMBER_OF_PACKETS packets to the NIC queue

						// set recordId which will be used to determine if there is a need to read more
						// packets after the releasing resources of BUFFER_NUMBER_OF_PACKETSth packet
						for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
							// read each record separately.
							hd_.work(HD_READ_MAP_OUTPUT, (applications.at(appID))->getRecordSize(), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
					}
					else{	// send all packets to the NIC queue

						for(int i=0;i<numberOfMergeRecords;i++){
							// read each record separately.
							hd_.work(HD_READ_MAP_OUTPUT, (applications.at(appID))->getRecordSize(), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
						if(lastRecordExist){
							hd_.work(HD_READ_MAP_OUTPUT, lastRecordSize, ev, outputEventType_);
							ev->incGlobalEntityId();
						}
					}
					// save remaining number of packets to send and the information whether a last packet exists and the lastpacketsize
					if(lastRecordExist){
						(applications.at(appID))->setMapMergeInfo(ev->getFsId(), ev->getRedId(), numberOfMergeRecords+1, lastRecordExist, lastRecordSize);
					}
					else{
						(applications.at(appID))->setMapMergeInfo(ev->getFsId(), ev->getRedId(), numberOfMergeRecords, lastRecordExist, lastRecordSize);
					}
				}
			}
		}
		else if(currentEvent == MERGE_PARTITIONS_FOR_REDUCER){ /*DOC: merge partitions for each reducer*/
			// release disk resource and check waiting queue for read request for mapper
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			// release seized hd resources in remote node
			if(ev->getRecordId() == 1){	// there are more packets that were not able to fit in buffer

				if ((applications.at(appID))->incMergeBufferCompletedPacketCount(ev->getFsId(), ev->getRedId()) == BUFFER_NUMBER_OF_PACKETS){

					// update the remaining number of transfer records
					(applications.at(appID))->decrementMergeInfoNumberOfMapRecords(ev->getFsId(), ev->getRedId(),BUFFER_NUMBER_OF_PACKETS);

					// get the remaining number of transfer records
					int remaining = (applications.at(appID))->getMergeInfo_remainingNumberOfMapRecords(ev->getFsId(), ev->getRedId());

					ev->setSeizedResQuantity(0);

					if(remaining > BUFFER_NUMBER_OF_PACKETS){	// not the last set

						for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
							// read filesplit from remote and send a response back to the origin

							// send each record separately.
							hd_.work(HD_READ_MAP_OUTPUT, (applications.at(appID))->getRecordSize(), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
					}
					else{	// it is the last set
						// last set of records!
						ev->setRecordId(-1);

						for(int i=0;i<remaining;i++){
							// read filesplit from remote and send a response back to the origin
							// send each record separately.
							hd_.work(HD_READ_MAP_OUTPUT, (applications.at(appID))->getRecordSize(), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
						if((applications.at(appID))->getMergeInfo_lastRecordExist(ev->getFsId(), ev->getRedId())){
							hd_.work(HD_READ_MAP_OUTPUT, (applications.at(appID))->getMergeInfo_lastRecordSize(ev->getFsId(), ev->getRedId()), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
					}

					// reset bufferCompletedPacketCount
					(applications.at(appID))->resetMergeBufferCompletedPacketCount(ev->getFsId(), ev->getRedId());
				}
			}
			else{	// the last set of input. just release...

				if(!(applications.at(appID))->decrementMergeInfoNumberOfMapRecords(ev->getFsId(), ev->getRedId(),1)){	// done

					// start processing the data (merge)
					// create an event to call event START_REDUCE_SORT_FUNC
					Event newEvent(appID, ev->getEventTime(), (applications.at(appID))->getReducerPartitionSize(ev->getFsId()), 0, ev->getDestEventType(), ev->getDestEventType(),
							CPU_MAP_MERGE, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());

					eventsList.push(newEvent);
				}
			}
		}
		else if(currentEvent == CPU_MAP_MERGE){	/*DOC: mapper merge in cpu*/
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: MERGE IN CPU_____TIME:   " << ev->getEventTime() << "attribute: " << ev->getEntityIns().getAttribute() <<  " fsID " << ev->getFsId() <<
					" --needed " << ev->getNeededResQuantity() <<  " seized: "<< ev->getSeizedResQuantity() << std::endl;
			std::cout << "Merge Read Complete: " << ev->getFsId() <<  "  " << ev->getEventTime() << " redId " << ev->getRedId()<< std::endl;
			#endif

			// start measuring CPU merge function time.
			(applications.at(appID))->addMergeFnStartTime(ev->getFsId(), ev->getEventTime());
			cpu_.mergeWork(ev, 2*(applications.at(appID))->getMapIntensity());
		}
		else if(currentEvent == MAP_MERGE_WB){	/*DOC: map task write back */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: MERGE IN WRITEBACK_____TIME:   " << ev->getEventTime() << " fsID " << ev->getFsId() << " seized: "<< ev->getSeizedResQuantity() <<
					" @needed: " << ev->getNeededResQuantity() << std::endl;
			std::cout << "NODE||INFO: Merge CPU Process Complete: " << ev->getFsId() <<  "  " << ev->getEventTime() << "for reducer " << ev->getRedId() <<  std::endl;
			#endif

			cpu_.decCpuUserCount(ev->getFsId(), ev->getAppID());

			// delete from the map of active maps in this node
			if ( activeMappersInThisNode_.find(std::make_pair(ev->getFsId(),ev->getAppID())) != activeMappersInThisNode_.end() ) {	// found
				activeMappersInThisNode_.erase(std::make_pair(ev->getFsId(),ev->getAppID()));
				(applications.at(appID))->incCompletedMapperCount();
			}
			// again here write each part instead of writing all at once...
			double parititonOfEachReducer = (applications.at(appID))->getReducerPartitionSize(ev->getFsId());

			ev->setSeizedResQuantity(0);

			int numberOfMergeRecords = parititonOfEachReducer/((applications.at(appID))->getRecordSize());

			bool lastRecordExist = false;
			double lastRecordSize=0;
			if((size_t)numberOfMergeRecords*((applications.at(appID))->getRecordSize()) < parititonOfEachReducer){
				lastRecordSize = parititonOfEachReducer - (size_t)numberOfMergeRecords*((applications.at(appID))->getRecordSize());
				lastRecordExist=true;
			}
			if(lastRecordExist){
				(applications.at(appID))->setMapMergeRecordCount(ev->getFsId(), numberOfMergeRecords+1);
			}
			else{
				(applications.at(appID))->setMapMergeRecordCount(ev->getFsId(), numberOfMergeRecords);
			}

			// set recordId which will be used to determine if there is a need to read more
			// packets after the releasing resources of BUFFER_NUMBER_OF_PACKETSth packet
			if(numberOfMergeRecords > BUFFER_NUMBER_OF_PACKETS){
				ev->setRecordId(MORE_PACKETS);
			}
			else{
				ev->setRecordId(-1);
			}

			if(numberOfMergeRecords > BUFFER_NUMBER_OF_PACKETS){

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					// write each record separately.
					hd_.work(HD_WRITE_MERGED_DATA, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
			else{
				for(int i=0;i<numberOfMergeRecords;i++){
					// write each record separately.
					hd_.work(HD_WRITE_MERGED_DATA, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				if(lastRecordExist){
					hd_.work(HD_WRITE_MERGED_DATA, lastRecordSize, ev, outputEventType_);
				}
			}
			// save remaining number of packets to send and the information whether a last packet exists and the lastpacketsize
			if(lastRecordExist){
				(applications.at(appID))->setMapMergeInfo(ev->getFsId(), ev->getRedId(), numberOfMergeRecords+1, lastRecordExist, lastRecordSize);
			}
			else{
				(applications.at(appID))->setMapMergeInfo(ev->getFsId(), ev->getRedId(), numberOfMergeRecords, lastRecordExist, lastRecordSize);
			}
		}
		else if(currentEvent == MAP_MERGE_WB_COMPLETE){	/*DOC: map task write back completed release resources (ready for reducer(s))*/
			// release disk resource and check waiting queue for write request
			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			bool donewriting = false;

			if(ev->getRecordId() == 1){	// there are more packets that were not able to fit in buffer

				if ((applications.at(appID))->incMergeBufferCompletedPacketCount(ev->getFsId(), ev->getRedId()) == BUFFER_NUMBER_OF_PACKETS){

					// update the remaining number of transfer records
					(applications.at(appID))->decrementMergeInfoNumberOfMapRecords(ev->getFsId(), ev->getRedId(),BUFFER_NUMBER_OF_PACKETS);

					// get the remaining number of transfer records
					int remaining = (applications.at(appID))->getMergeInfo_remainingNumberOfMapRecords(ev->getFsId(), ev->getRedId());

					ev->setSeizedResQuantity(0);

					if(remaining > BUFFER_NUMBER_OF_PACKETS){	// not the last set

						for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
							// read filesplit from remote and send a response back to the origin

							// send each record separately.
							hd_.work(HD_WRITE_MERGED_DATA, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
					}
					else{	// it is the last set
						// last set of records!
						ev->setRecordId(-1);

						for(int i=0;i<remaining;i++){
							// read filesplit from remote and send a response back to the origin
							// send each record separately.

							hd_.work(HD_WRITE_MERGED_DATA, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
						if((applications.at(appID))->getMergeInfo_lastRecordExist(ev->getFsId(), ev->getRedId())){

							hd_.work(HD_WRITE_MERGED_DATA, (applications.at(appID))->getMergeInfo_lastRecordSize(ev->getFsId(), ev->getRedId()), ev, outputEventType_);
						}
					}
					// reset bufferCompletedPacketCount
					(applications.at(appID))->resetMergeBufferCompletedPacketCount(ev->getFsId(), ev->getRedId());
				}
			}
			// the last set of input. just release...
			else if(!(applications.at(appID))->decrementMergeInfoNumberOfMapRecords(ev->getFsId(), ev->getRedId(),1)){	// done
				// done writing
				donewriting = true;
				ev->setNeededResQuantity((applications.at(appID))->getReducerPartitionSize(ev->getFsId()));
			}
			// All the parts of reduce partition with this fsid is written after this point
			if(donewriting && (applications.at(appID))->notifyMapMergeComplete(ev->getFsId())){	// all partitions for each reducer are merged
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "NODE||INFO: MERGE WRITEBACK COMPLETE_____TIME:   " << ev->getEventTime() << " fsID " << ev->getFsId() << " seized: "<< ev->getSeizedResQuantity() << std::endl;
				std::cout << "NODE||INFO: AppID: " << appID << " ~Mapper ID: " << ev->getFsId() << " finish time: " << ev->getEventTime() << " QueueID: "  << (applications.at(appID))->getQueueId() << std::endl;
				#endif

				(applications.at(appID))->addMapFinishTime(ev->getFsId(), ev->getEventTime());
				(applications.at(appID))->notifyWbCompleteMappers();
				// add created data to the list of completed mappers
				(applications.at(appID))->saveReadyToShuffleInfo(ev->getFsId(), ev->getDestEventType(), ev->getNeededResQuantity());

				// READY FOR REDUCER PHASE!


				// decrease active map task count
				g_scheduler.decrementActiveTaskCount(appID, true);

				// release map task resources
				memory_.setRemainingCapacity(memory_.getRemainingCapacity() + (applications.at(appID))->getSeizedMapreduceMapMemory());
				struct waitingTaskInfo waitingMapper = g_scheduler.schedulerReleaseMapperresources(appID, ev->getDestEventType(), ev->getFsId());

				if(waitingMapper.taskLocation != -2){
					if(waitingMapper.taskType){	// reducer
						#ifndef TERMINAL_LOG_DISABLED
						std::cout << "NODE||INFO: Create reducer at "<< waitingMapper.taskLocation << " upon releasing a mapper at location: "<< ev->getDestEventType()  << std::endl;
						#endif
						(applications.at(waitingMapper.appId))->addReducerLocations(waitingMapper.taskLocation);

						Event newEvent(waitingMapper.appId, ev->getEventTime()+OVERHEAD_DELAY, HEARTBEAT_SIZE, 0.0, waitingMapper.outEvent, (applications.at(waitingMapper.appId))->getAmEventType(),
								START_REDUCE_CONTAINER, OTHER, SEIZETOMASTER, waitingMapper.taskLocation);

						eventsList.push(newEvent);
					}
					else{	// map task
						// attribute carries mapperLocation
						#ifndef TERMINAL_LOG_DISABLED
						std::cout << "NODE||INFO: Create mapper at "<< waitingMapper.taskLocation << " upon releasing a mapper at location: "<< ev->getDestEventType()  << std::endl;
						#endif

						Event newEvent(waitingMapper.appId, (ev->getEventTime()+OVERHEAD_DELAY),HEARTBEAT_SIZE, 0.0, waitingMapper.outEvent, (applications.at(waitingMapper.appId))->getAmEventType(),
								START_MAP_CONTAINER, OTHER, SEIZETOMASTER, waitingMapper.taskLocation, waitingMapper.attr, waitingMapper.fsid);

						eventsList.push(newEvent);
					}
				}

				if((applications.at(appID))->shuffleReady()){
					// when number of completed map tasks are sufficient to start shuffle (based on slow start), generate reducers...
					if(!(applications.at(appID))->isReducersRequested()){
						// send Reducer Requests
						#ifndef TERMINAL_LOG_DISABLED
						std::cout<< "NODE||INFO: Generate Reducer Requests for Application: " << appID << std::endl;
						#endif
						(applications.at(appID))->setReducersRequested(true);

						for(int j=0;j<(applications.at(appID))->getReduceCount();j++){

							Event newEvent(appID, ev->getEventTime()+((j)*0.0001),HEARTBEAT_SIZE,
									0.0, outputEventType_, (applications.at(appID))->getRmEventType(), ALLOCATE_REDUCE_RESOURCES_RM, MAPTASK);

							eventsList.push(newEvent);
						}
					}

					// set map finish time
					if((applications.at(appID))->getWbCompleted() == (applications.at(appID))->getTotalNumberOfMappers()){
						(applications.at(appID))->setMapFinishTime(ev->getEventTime());
					}

					// get total number of reducers (ASSUMPTION: the data will be evenly distributed among reducers...)
					int totalReducers = (applications.at(appID))->getReduceCount();
					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "NODE||INFO: totalReducers " << totalReducers  << std::endl;
					#endif
					size_t numreadyMappers = (applications.at(appID))->getNumberOfReadyMappers();

					for(size_t j=0;j<numreadyMappers;j++){

						spillInfo toBeShuffled = (applications.at(appID))->popReadyToShuffleInfo();
						for(int i=0;i< totalReducers;i++){
							// create events to start shuffling for the mappers at their corresponding nodes with the appID (if not already shuffled)
							// store reducer order (to determine which reducer to send data to) in event attribute
							Event newEvent(appID, ev->getEventTime(), toBeShuffled.size_, 0, toBeShuffled.nodeEventType_, toBeShuffled.nodeEventType_,
									START_SHUFFLE, MAPTASK, SEIZETOMASTER, i, -1, toBeShuffled.fsID_);

							eventsList.push(newEvent);
						}
					}
				}
			}	/* finish all partitions for each reducer are merged */
		}
		/*--------------------------------------------------------------------------------------------------------------------------------*/
		/*-----------------------------------------------------MAP PHASE IS COMPLETED-----------------------------------------------------*/
		/*--------------------------------------------------------------------------------------------------------------------------------*/
		else if(currentEvent == START_SHUFFLE){	/*DOC: start shuffling */

			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: AppID " << appID << "+++++++SHUFFLING: READ+++++++TIME:  " << ev->getEventTime() << " NodeId: " << ev->getDestEventType() << " fsID " << ev->getFsId() << " seized: "<< ev->getSeizedResQuantity() << std::endl;
			#endif

			// add to a queue of "reducer creation completion waiters"
			(applications.at(appID))->saveReducerCompletionWaiters(ev->getNeededResQuantity(), ev->getDestEventType(), ev->getEntityIns().getAttribute(), ev->getFsId(), outputEventType_);

			// make sure at least one reducer is created
			if((applications.at(appID))->hasAnyReducerCreated()){

				// Overall shuffle start time
				int reducerLocationIndex = ev->getEntityIns().getAttribute();
				if((applications.at(appID))->getShuffleStartTime(reducerLocationIndex) < 0){
					(applications.at(appID))->setShuffleStartTime(ev->getEventTime(), reducerLocationIndex);
				}

				if((applications.at(appID))->isReducerCompletionWaitersHasElement()){	// then there are some map output waiting to be shuffled to reducers
					size_t totalwaiters= (applications.at(appID))->gettotalReducerCompletionWaiters();

					for(size_t i=0; i < totalwaiters;i++){

						reducerWaiterData waiterData = (applications.at(appID))->getReducerCompletionWaiter(i);
						int finalDest = (applications.at(appID))->getReducerLocation(waiterData.attribute_);

						if(finalDest != -1){	// reducer location found (which means reducer was created) and data was not shuffled (since it was waiting in waiterData)

							// add shuffle start time (per shuffle)
							(applications.at(appID))->addShuffleStartTime(waiterData.fsID_, waiterData.attribute_, ev->getEventTime());
							// read data at corresponding node to be sent to a reducer (waiterData.attribute_ will be used to get reducer location)
							Event newEvent(appID, ev->getEventTime(), waiterData.neededResQuantity_, 0, waiterData.myLoc_, waiterData.myLoc_,
									SHUFFLE_READ_DATA, REDUCETASK, SEIZETOMASTER, waiterData.attribute_, -1, waiterData.fsID_, waiterData.attribute_);
							eventsList.push(newEvent);

							// data shuffle started so set shuffle flag to true
							(applications.at(appID))->signalReducerCompletionWaiter(i);
						}
					}
					// clear shuffle started elements from reducer completion waiter list (those who wait fro reducer to be created to start shuffle...)
					(applications.at(appID))->clearFinishedReducerCompletionWaiters();
				}
			}
		}

		else if(currentEvent == SHUFFLE_READ_DATA){	/*DOC: SHUFFLE  - Read at remote for waiters*/
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: AppID " << appID  << " SHUFFLE READ FOR WAITERS at :" << ev->getRedId() << " TIME: "<< ev->getEventTime() << " BYTES TO BE PROCESSED: "<< ev->getNeededResQuantity() << std::endl;
			#endif

			// read each packet separately from local disk
			// (packets are assumed to be of size ((applications.at(appID))->getRecordSize()))

			double totalSplitSize = ev->getNeededResQuantity();

			// this is experimental
			(applications.at(appID))->setShuffleReadStartTime(ev->getFsId(), ev->getEventTime());
			// this will be checked to confirm that a reduce task receives all data in this fsId that was intended for it.
			(applications.at(appID))->setShuffledTotalDataForFsidRedid(ev->getFsId(), ev->getRedId(), totalSplitSize);

			int numberOfPackets = totalSplitSize/((applications.at(appID))->getRecordSize());
			bool lastRecordExist = false;
			double lastRecordSize;

			if((size_t)numberOfPackets*((applications.at(appID))->getRecordSize()) < totalSplitSize){
				lastRecordSize = totalSplitSize - numberOfPackets*((applications.at(appID))->getRecordSize());
				lastRecordExist=true;
			}
			if(lastRecordExist){
				(applications.at(appID))->setShufflePacketCount(ev->getFsId(), ev->getRedId(), numberOfPackets+1);
			}
			else{
				(applications.at(appID))->setShufflePacketCount(ev->getFsId(), ev->getRedId(), numberOfPackets);
			}

			if(numberOfPackets > BUFFER_NUMBER_OF_PACKETS){
				ev->setRecordId(MORE_PACKETS);

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					hd_.work(HD_READ_DATA_TO_SHUFFLE, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
			else{
				ev->setRecordId(-1);

				for(int i=0;i<numberOfPackets;i++){
					hd_.work(HD_READ_DATA_TO_SHUFFLE, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}

				if(lastRecordExist){
					hd_.work(HD_READ_DATA_TO_SHUFFLE, lastRecordSize, ev, outputEventType_);
				}
			}
			// save remaining number of packets to send and the information whether a last packet exists and the lastpacketsize
			(applications.at(appID))->setShuffleReadInfo(ev->getFsId(), ev->getRedId(), numberOfPackets, lastRecordExist, lastRecordSize);
		}

		else if(currentEvent == SHUFFLE_READ_DATA_COMPLETE){ 	/*DOC: start shuffling */
			#ifndef TERMINAL_LOG_DISABLED
						std::cout << "NODE||INFO: AppID " << appID << " SHUFFLING: TRANSFER_____TIME:  " << ev->getEventTime() << " NodeId: " << ev->getDestEventType() << " fsID " << ev->getFsId() <<
								" seized: "<< ev->getSeizedResQuantity() << " needed: " << ev->getNeededResQuantity() <<  std::endl;
			#endif

			int reducerLocationIndex = ev->getEntityIns().getAttribute();
			int finalDest = (applications.at(appID))->getReducerLocation(reducerLocationIndex);
			// transfer data to reducer(s) & release read resources for shuffle
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			double shuffleOutputPartSize = ev->getNeededResQuantity();

			if(ev->getDestEventType() == finalDest){	// local
				// now each part can be sent to a reducer
				Event newEvent(appID, ev->getEventTime(), shuffleOutputPartSize, 0, finalDest, finalDest,
						SHUFFLE_IN_MEM_COLLECTION, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), -1, ev->getFsId(), reducerLocationIndex);

				eventsList.push(newEvent);
			}
			else{
				// now each part can be sent to a reducer
				Event newEvent(appID, ev->getEventTime(), shuffleOutputPartSize, 0, outputEventType_, finalDest,
						SHUFFLE_IN_MEM_COLLECTION, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), -1, ev->getFsId(), reducerLocationIndex);

				eventsList.push(newEvent);
			}

			// check if there are more to read for this fsid and redid
			if(ev->getRecordId() == 1 &&
					(applications.at(appID))->incShuffleReadBufferCompletedPacketCount(ev->getFsId(), ev->getRedId()) == BUFFER_NUMBER_OF_PACKETS){
				// there are more packets that were not able to fit in buffer
				// update the remaining number of transfer records

				(applications.at(appID))->decShuffleReadNumberOfRecords(ev->getFsId(), ev->getRedId(),BUFFER_NUMBER_OF_PACKETS);

				// get the remaning number of transfer records
				int remaining = (applications.at(appID))->getShuffleReadInfo_remainingNumberOfMapRecords(ev->getFsId(), ev->getRedId());

				if(remaining > BUFFER_NUMBER_OF_PACKETS){	// not the last set

					ev->setSeizedResQuantity(0);
					for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
						// read filesplit from remote and send a response back to the origin

						// send each record separately.
						hd_.work(HD_READ_DATA_TO_SHUFFLE, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
						ev->incGlobalEntityId();
					}
				}
				else{	// it is the last set
					// last set of records!
					ev->setRecordId(-1);
					ev->setSeizedResQuantity(0);

					for(int i=0;i<remaining;i++){
						// read filesplit from remote and send a response back to the origin
						// send each record separately.

						hd_.work(HD_READ_DATA_TO_SHUFFLE, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
						ev->incGlobalEntityId();
					}
					if((applications.at(appID))->getShuffleReadInfo_lastRecordExist(ev->getFsId(), ev->getRedId())){
						hd_.work(HD_READ_DATA_TO_SHUFFLE, (applications.at(appID))->getShuffleReadInfo_lastRecordSize(ev->getFsId(), ev->getRedId()), ev, outputEventType_);
					}
				}
				// reset bufferCompletedPacketCount
				(applications.at(appID))->resetShuffleReadBufferCompletedPacketCount(ev->getFsId(), ev->getRedId());
			}

			if(ev->getRecordId() == -1){
				// this is experimental
				(applications.at(appID))->setShuffleReadFinTime(ev->getFsId(), ev->getEventTime());
			}
		}
		else if(currentEvent == SHUFFLE_WRITE_DATA){	/*DOC: start shuffling write (now the transferred data is written to REDUCER node) */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: AppID " << appID  << " SHUFFLING: WRITE_____TIME:   " << ev->getEventTime() << " NodeId: " <<
					ev->getDestEventType() << " needed " << ev->getNeededResQuantity()<< " fsID " << ev->getFsId() << " seized: "<< ev->getSeizedResQuantity() << std::endl;
			#endif

			double totalSplitSize = ev->getNeededResQuantity();
			int numberOfPackets = totalSplitSize/((applications.at(appID))->getRecordSize());
			bool lastRecordExist = false;
			double lastRecordSize;

			if((size_t)numberOfPackets*((applications.at(appID))->getRecordSize()) < totalSplitSize){
				lastRecordSize = totalSplitSize - numberOfPackets*((applications.at(appID))->getRecordSize());
				lastRecordExist=true;
			}
			size_t myId = getUniqueId();
			ev->setSpillTally(myId);

			if(lastRecordExist){
				// this will be checked to confirm that a reduce task receives all data in this fsId that was intended for it.
				(applications.at(appID))->setShuffleWriteDataProperties(myId, numberOfPackets+1, totalSplitSize, lastRecordExist, lastRecordSize);
			}
			else{
				// this will be checked to confirm that a reduce task receives all data in this fsId that was intended for it.
				(applications.at(appID))->setShuffleWriteDataProperties(myId, numberOfPackets, totalSplitSize, lastRecordExist, lastRecordSize);
			}
			if(numberOfPackets > BUFFER_NUMBER_OF_PACKETS){
				ev->setRecordId(MORE_PACKETS);

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
			else{
				ev->setRecordId(-1);

				for(int i=0;i<numberOfPackets;i++){
					hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				if(lastRecordExist){
					hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, lastRecordSize, ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
		}
		else if(currentEvent == SHUFFLE_WRITE_DATA_PARTIALLY_COMPLETE){
			// this will generate SHUFFLE_WRITE_DATA_COMPLETE event when all the parts of data is received
			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			if(ev->getRecordId() == 1 && (applications.at(appID))->incShuffleWriteDataCount(ev->getSpillTally()) == BUFFER_NUMBER_OF_PACKETS){

				// there are more packets that were not able to fit in buffer
				// update the remaining number of transfer records
				(applications.at(appID))->decrementShuffleWriteDataRecords(ev->getSpillTally(),BUFFER_NUMBER_OF_PACKETS);

				// get the remaining number of transfer records
				int remaining = (applications.at(appID))->getShuffleWriteData_numPackets(ev->getSpillTally());

				ev->setSeizedResQuantity(0);
				if(remaining > BUFFER_NUMBER_OF_PACKETS){	// not the last set

					for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
						// read filesplit from remote and send a response back to the origin

						// send each record separately.
						hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
						ev->incGlobalEntityId();
					}
				}
				else{	// it is the last set
					// last set of records!
					ev->setRecordId(-1);

					for(int i=0;i<remaining;i++){
						// read filesplit from remote and send a response back to the origin
						// send each record separately.

						hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, ((applications.at(appID))->getRecordSize()), ev, outputEventType_);
						ev->incGlobalEntityId();
					}
					if((applications.at(appID))->getShuffleWriteData_lastRecordExist(ev->getSpillTally())){

						hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, (applications.at(appID))->getShuffleWriteData_lastRecordSize(ev->getSpillTally()), ev, outputEventType_);
					}
				}
				// reset bufferCompletedPacketCount
				(applications.at(appID))->resethuffleWriteDataCount(ev->getSpillTally());
			}

			if(ev->getRecordId() == -1){
				if(!(applications.at(appID))->decrementShuffleWriteDataRecords(ev->getSpillTally(),1)){	// done

					// start processing the data (merge)
					// create an event to call event START_REDUCE_SORT_FUNC
					Event newEvent(appID, ev->getEventTime(), (applications.at(appID))->getShuffleWriteDataProperties_DataSize(ev->getSpillTally()), 0, ev->getDestEventType(), ev->getDestEventType(),
							SHUFFLE_WRITE_DATA_COMPLETE, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());

					eventsList.push(newEvent);
				}
			}
		}
		else if(currentEvent == ON_DISK_MERGE_READ_COMPLETE){	/*DOC: Read Completed for On Disk Merge data from hard disk */
			// release On Disk Merge Data resources from HD
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			cpu_.reduceMergeWork(ev->getNeededResQuantity(), (applications.at(appID))->getSeizedReduceCpuVcores(), ev, 0.1);
		}
		else if(currentEvent == SHUFFLE_WRITE_DATA_COMPLETE){	/*DOC: Shuffle Releases write resources. */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: AppID " << appID  << " SHUFFLING: WRITE COMPLETED at " << ev->getRedId() <<  " TIME:   " << ev->getEventTime() << " NodeId: " << ev->getDestEventType() << " fsID " << ev->getFsId() <<
					" seized: "<< ev->getSeizedResQuantity() << std::endl;
			#endif

			// add data to the waiting list for merge
			(applications.at(appID))->adddataWaitingToBeMerged_(ev->getDestEventType(), ev->getNeededResQuantity(), ev->getRedId());
			(applications.at(appID))->notifyReduceWriteComplete(ev->getDestEventType(), ev->getRedId());

			// get each shuffle time
			int mergeID = ev->getEntityIns().getAttribute();
			if(mergeID != -1){	// request not coming from ON-DISK MERGE

			}
			else{
				#ifndef TERMINAL_LOG_DISABLED
				std::cout<< "NODE||INFO: AppID: " << appID<< "ON DISK MERGE COMPLETED " << ev->getEventTime() << " redId " << ev->getRedId() << std::endl;
				#endif

				int remainingReadyMergeCount = (applications.at(appID))->subReadyForOnDiskMergeCount_(ev->getRedId());

				if(remainingReadyMergeCount > 0){
					#ifndef TERMINAL_LOG_DISABLED
					std::cout<< "NODE||INFO: AppID: " << appID<< "ON DISK MERGE STARTED " << ev->getEventTime() << " redId " << ev->getRedId() << std::endl;
					#endif
					size_t totalSizeOfNewMergedFile = (applications.at(appID))->popMergeSize(ev->getDestEventType(), ev->getRedId());

					ev->setNeededResQuantity(totalSizeOfNewMergedFile);
					// set attribute to -1 to let shuffle time saver know that request is coming from on disk merge...
					ev->setEntityInsAttribute(-1);

					// read On Disk Merge Data
					hd_.work(ON_DISK_MERGE_READ_COMPLETE, totalSizeOfNewMergedFile, ev, outputEventType_);
				}
			}

			// if the number of files on disk is greater than  2 * mapreduce.task.io.sort.factor - 1, then start "on-disk merge" of mapreduce.task.io.sort.factor files
			size_t numberofWaiting2BeMerged = (applications.at(appID))->getNumberOfWaitingTobeMerged(ev->getDestEventType(), ev->getRedId());
			int io_sortFactor = (applications.at(appID))->getMapReduceConfig().getMapreducetaskIoSortFactor();

			if((applications.at(appID))->datareceivedFromAllMappers(ev->getDestEventType(), ev->getRedId()) &&
				(applications.at(appID))->getReduceSpillCount(ev->getDestEventType(), ev->getRedId()) ==
				(applications.at(appID))->getReduceWriteCount(ev->getDestEventType(), ev->getRedId())
				){ // perform the reduce SORT and pass the output to reducer
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "NODE||INFO: AppID: " << appID << "START REDUCE SORT " << ev->getEventTime() << " Reducer: "   <<  ev->getRedId() << " Location "  << ev->getDestEventType() << std::endl;
				#endif

				(applications.at(appID))->setShuffleFinishTime(ev->getEventTime(), ev->getRedId());

				// reduce start (per reducer)
				// the last non-On-disk write will set this value for each reducer
				(applications.at(appID))->addReduceStartTime(ev->getRedId(), ev->getEventTime());

				// send numberofWaiting2BeMerged within attribute of an event
				Event newEvent(appID, ev->getEventTime(), -1, -1, ev->getDestEventType(), ev->getDestEventType(),
						START_REDUCE_SORT_FUNC, REDUCETASK, SEIZETOMASTER, numberofWaiting2BeMerged, ev->getFsLoc(), ev->getFsId(), ev->getRedId());

				eventsList.push(newEvent);

				//reset numberofWaiting2BeMerged
				numberofWaiting2BeMerged = 0;
			}

			if(numberofWaiting2BeMerged >= (2 * (size_t)io_sortFactor)-1){
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "NODE||INFO: AppID: " << appID<< " ON DISK MERGE!"  << ev->getRedId() << " numberofWaiting2BeMerged " << numberofWaiting2BeMerged<< std::endl;
				#endif

				// merge the last "mapreduce.task.io.sort.factor" files
				bool isOngoingOnDiskMerge = (applications.at(appID))->addReadyForOnDiskMergeCount_(ev->getRedId());

				// reduce ready to be merged count
				(applications.at(appID))->reduceNumberOfChunksWaitingToBeMerged(ev->getDestEventType(), ev->getRedId());


				// notify future on disk merge write
				(applications.at(appID))->notifyIncomingOnDiskMergeWrite(ev->getDestEventType(), ev->getRedId());

				if(!isOngoingOnDiskMerge){

					size_t totalSizeOfNewMergedFile = (applications.at(appID))->popMergeSize(ev->getDestEventType(), ev->getRedId());

					ev->setNeededResQuantity(totalSizeOfNewMergedFile);
					// set attribute to -1 to let shuffle time saver know that request is coming from on disk merge...
					ev->setEntityInsAttribute(-1);

					// read On Disk Merge Data
					hd_.work(ON_DISK_MERGE_READ_COMPLETE, totalSizeOfNewMergedFile, ev, outputEventType_);
				}
			}
			// otherwise wait for completion of shuffle. when shuffle is completed, "sort" phase of reduce will begin (actually "final merge" is a better name for this phase )
		}

		else if(currentEvent == START_REDUCE_SORT_FUNC){	// reduce sort (will repetitively be called until remaining files per reducer are less than mapreduce.task.io.sort.factor)

			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: AppID: " << appID<< " START_REDUCE_SORT_FUNC!"  << ev->getRedId() << " TIME: " <<  ev->getEventTime()<< std::endl;
			#endif
			int io_sortFactor = (applications.at(appID))->getMapReduceConfig().getMapreducetaskIoSortFactor();
			int numberofWaiting2BeMerged = ev->getEntityIns().getAttribute();

			if(numberofWaiting2BeMerged <= io_sortFactor){	// now feed the output of sort to reduce function

				// reduce start (overall)
				if((applications.at(appID))->getReduceStartTime() < 0){
					(applications.at(appID))->setReduceStartTime(ev->getEventTime());
				}
				// here read the shuffled data for reduceFunctionWork

				// calculate reduce internal output partition size that needs to be read from local hd by this reducer
				size_t reduceOutputPartSize = ((applications.at(appID))->getAppSize() * ((applications.at(appID))->getReduceOutputVolume())/100.0) / (applications.at(appID))->getReduceCount();

				recordSetter (reduceOutputPartSize, (applications.at(appID))->getRecordSize(), ev->getRedId(), false, ev);
			}
			else if(numberofWaiting2BeMerged >= 2*io_sortFactor-1){		// sort& merge 10 files
				// upon return this will be numberofWaiting2BeMerged (put this into the attribute of the event)
				numberofWaiting2BeMerged -= (io_sortFactor-1);

				size_t totalSizeOfNewMergedFile = (applications.at(appID))->popMergeSize(ev->getDestEventType(), ev->getRedId());

				ev->setNeededResQuantity(totalSizeOfNewMergedFile);
				ev->setEntityInsAttribute(numberofWaiting2BeMerged);

				// read ToBeSorted Reduce Data
				hd_.work(HD_READ_TOBE_SORTED_REDUCE_DATA, totalSizeOfNewMergedFile, ev, outputEventType_);
			}
			else{	// numberofWaiting2BeMerged < 2*io_sortFactor-1 && numberofWaiting2BeMerged > io_sortFactor

				int toBeMergedCount = 1+ numberofWaiting2BeMerged-io_sortFactor;

				// upon return this will be numberofWaiting2BeMerged (put this into the attribute of the event)
				numberofWaiting2BeMerged -= (numberofWaiting2BeMerged-io_sortFactor);

				size_t totalSizeOfNewMergedFile = (applications.at(appID))->popMergeGivenSize(ev->getDestEventType(), ev->getRedId(), toBeMergedCount);

				ev->setNeededResQuantity(totalSizeOfNewMergedFile);
				ev->setEntityInsAttribute(numberofWaiting2BeMerged);

				// read ToBeSorted Reduce Data
				hd_.work(HD_READ_TOBE_SORTED_REDUCE_DATA, totalSizeOfNewMergedFile, ev, outputEventType_);
			}
		}
		else if(currentEvent == RELEASE_AND_START_REDUCE_SORT){	// release and start reduce sort

			// release ToBeSorted Reduce Data resources from HD
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			cpu_.reduceSort((applications.at(appID))->getSeizedReduceCpuVcores(), ev, (applications.at(appID))->getReduceSortIntensity());
		}
		else if(currentEvent == RELEASE_AND_FINISH_REDUCE_SORT){	// release and finish reduce sort
			cpu_.decreduceCpuUserCount(ev->getRedId(), ev->getAppID());
			// write output to disk and upon completion of disk write create new reduce sort event (event START_REDUCE_SORT_FUNC)
			// write sorted Reduce Data to disk
			hd_.work(HD_WRITE_SORTED_REDUCE_DATA, ev->getNeededResQuantity(), ev, outputEventType_);
		}
		else if(currentEvent == RELEASE_AND_DONE_REDUCE_SORT){	// release and finish reduce sort

			// release write resources for sorted Reduce Data
			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			// push new file on waiting to be merged
			(applications.at(appID))->adddataWaitingToBeMerged_(ev->getDestEventType(), ev->getNeededResQuantity(), ev->getRedId());

			// create an event to call event START_REDUCE_SORT_FUNC
			Event newEvent(appID, ev->getEventTime(), ev->getNeededResQuantity(), 0, ev->getDestEventType(), ev->getDestEventType(),
					START_REDUCE_SORT_FUNC, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());

			eventsList.push(newEvent);
		}
		else if(currentEvent == READ_REDUCE_RECORDS_TO_SORT){
			recordInfo newRecord = (applications.at(appID))->getReduceRecordInfo(ev->getRedId());

			if(newRecord.remainingRecordCount_ > BUFFER_NUMBER_OF_PACKETS){	// send BUFFER_NUMBER_OF_PACKETS now. upon completion send more...
				// decrement remaining reduce record count
				(applications.at(appID))->decrementRecordInfoRemainingReduceRecordCountAmount(ev->getRedId(), BUFFER_NUMBER_OF_PACKETS);
				ev->setRecordId(newRecord.remainingRecordCount_);

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					// read each record and upon completion release the seized resource and start map function on the record just read
					hd_.work(HD_READ_SHUFFLED_DATA, newRecord.eachRecordSize_, ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
			else{
				// reset remaining reduce record count
				(applications.at(appID))->decrementRecordInfoRemainingReduceRecordCountAmount(ev->getRedId(), newRecord.remainingRecordCount_);

				ev->setRecordId(0);

				for(int i=0;i<newRecord.remainingRecordCount_;i++){

					hd_.work(HD_READ_SHUFFLED_DATA, newRecord.eachRecordSize_, ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				if(newRecord.lastRecordExist_){
					hd_.work(HD_READ_SHUFFLED_DATA, newRecord.lastRecordSize_, ev, outputEventType_);
				}
			}
			// if there is no other record to read for this filesplit do nothing...
		}
		else if(currentEvent == SHUFFLE_IN_MEM_COLLECTION){	/*DOC: Shuffle In-memory collection. */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: AppID " << appID << " +++++++SHUFFLING: IN-MEMORY COLLECTION:   " << ev->getEventTime() <<  " NodeId: " << ev->getDestEventType() << " fsID " << ev->getFsId() <<
					" reducerID: " << ev->getRedId() << " seized: "<< ev->getSeizedResQuantity() << std::endl;
			#endif

			/*
			 * There are 3 cases to consider:
			 *
			 * 1. isExceedingMaxSingleShuffleLimit: a single map task output to a specific reduce task,
			 * cannot contain more data than a certain percentage of buffer in case this limit is exceeded, spill is needed.
			 *
			 * 2. startMergeSpill: merge spill starts when the total accumulated data amount exceeds certain percentage
			 * of reduce task buffer (independent of which map task's output it is)
			 *
			 * 3. if all output of mappers is received, then a spill is created with the remaining data in the buffer
			 *
			 */

			// CASE1: isExceedingMaxSingleShuffleLimit
			bool isExceedingMaxSingleShuffleLimit = (applications.at(appID))->incShuffleCollectedDataAmount(ev->getFsId(), ev->getRedId(), ev->getNeededResQuantity());

			if(isExceedingMaxSingleShuffleLimit){
				(applications.at(appID))->incShuffleFlushedDataAmount(ev->getFsId(), ev->getRedId());
			}
			// CASE2: startMergeSpill
			bool startMergeSpill = memory_.addCheckMergeSpill(ev->getNeededResQuantity(), (applications.at(appID))->getShuffleMergeLimit(), ev->getRedId(), appID);

			// for each received packet, notify data reception (this is used to determine whether all packets from all map tasks are received)
			(applications.at(appID))->notifyDataReception(ev->getDestEventType(), ev->getRedId(), ev->getFsId());

			if(isExceedingMaxSingleShuffleLimit || startMergeSpill || (applications.at(appID))->datareceivedFromAllMappers(ev->getDestEventType(), ev->getRedId())){

				// total number of spills on disk for this reduce task
				(applications.at(appID))->notifyReduceSpillReception(ev->getDestEventType(), ev->getRedId());

				// more than 1 map outputs in memory (need to merge them)
				if(memory_.getnumberOfMapOutputsWaitingForSpill(ev->getRedId(), appID) > 1){
					cpu_.reduceMergeWork(memory_.getResetDataWaitingForSpill(ev->getRedId(), appID), (applications.at(appID))->getSeizedReduceCpuVcores(), ev, 0.1);
				}
				else{
					Event newEvent(appID, ev->getEventTime(), memory_.getResetDataWaitingForSpill(ev->getRedId(), appID), 0, ev->getDestEventType(), ev->getDestEventType(),
							SHUFFLE_WRITE_DATA, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());

					eventsList.push(newEvent);
				}
			}
		}
		else if(currentEvent == REDUCER_IN_MEM_INTER_MERGE){	/*DOC: REDUCER In-memory intermediate merge */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: APPID: " << appID << "+++++++SHUFFLING: IN-MEMORY PROCESSING...: TIME: "<< ev->getEventTime() << "NEEDED "<< ev->getNeededResQuantity() << " seized: "<< ev->getSeizedResQuantity() << std::endl;
			#endif
			cpu_.decreduceCpuUserCount(ev->getRedId(), ev->getAppID());

			Event newEvent(appID, ev->getEventTime(), ev->getNeededResQuantity(), 0, ev->getDestEventType(), ev->getDestEventType(),
					SHUFFLE_WRITE_DATA, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());

			eventsList.push(newEvent);
		}

		else if(currentEvent == START_REDUCE_FUNC){	/*DOC: Start reduce function */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: AppID: " << appID << " REDUCE FUNCTION STARTED at :" << ev->getRedId() << " TIME: "<< ev->getEventTime() << " BYTES TO BE PROCESSED: "<< ev->getNeededResQuantity() << std::endl;
			#endif

			// release "read shuffled output" resources in reducer
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			cpu_.reduceFunctionWork(ev->getNeededResQuantity(),
					(applications.at(appID))->getSeizedReduceCpuVcores(), ev, (applications.at(appID))->getReduceIntensity());
		}

		else if(currentEvent == FINISH_REDUCE_FUNC){	/*DOC: Finish reduce function */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "NODE||INFO: AppID: " << appID << " REDUCE FUNCTION COMPLETED at :" << ev->getRedId() << " TIME: "<< ev->getEventTime() << " BYTES TO BE WRITTEN: "<< ev->getNeededResQuantity() << " QueueID: " << (applications.at(appID))->getQueueId() << std::endl;
			#endif

			cpu_.decreduceCpuUserCount(ev->getRedId(), ev->getAppID());
			// calculate final output partition that needs to be written to HDFS by this reducer
			double finalOutputPartSize = ev->getNeededResQuantity()*((applications.at(appID))->getFinalOutputVolume()/(applications.at(appID))->getReduceOutputVolume());

			hd_.work(HD_WRITE_REDUCE_OUTPUT, finalOutputPartSize, ev, outputEventType_);
		}
		else if(currentEvent == WRITE_REDUCE_OUTPUT){	/*DOC: Reduce output written to output file! */

			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			// when all the reduce records arrive here, then reduce output completed writing its data for this reduce task..
			if((applications.at(appID))->areAllReduceRecordComplete(ev->getRedId())){
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "NODE||INFO: AppID: " << appID << " REDUCE OUTPUT WRITTEN TO FILE: TIME: "<< ev->getEventTime()  << " BYTES WRITTEN: "<< ev->getNeededResQuantity() <<
						" QueueID: "  << (applications.at(appID))->getQueueId() <<std::endl;
				#endif

				// add reduce finish time
				(applications.at(appID))->addReduceFinishTime(ev->getRedId(), ev->getEventTime());

				// this reducer has completed its task!
				(applications.at(appID))->notifyCompletedReducers();

				// decrease active reduce task count
				g_scheduler.decrementActiveTaskCount(appID, false);

				// release reducer resources
				memory_.setRemainingCapacity(memory_.getRemainingCapacity() + (applications.at(appID))->getSeizedMapreduceReduceMemory());
				struct waitingTaskInfo waitingReducer = g_scheduler.schedulerReleaseReducerresources(appID, ev->getDestEventType());

				if(waitingReducer.taskLocation != -2){
					if(waitingReducer.taskType){	// reducer

						(applications.at(waitingReducer.appId))->addReducerLocations(waitingReducer.taskLocation);
						#ifndef TERMINAL_LOG_DISABLED
						std::cout << "NODE||INFO: Create reducer at "<< waitingReducer.taskLocation << " upon releasing a reducer at location: "<< ev->getDestEventType()  << std::endl;
						#endif
						Event newEvent(waitingReducer.appId, (ev->getEventTime()+OVERHEAD_DELAY),HEARTBEAT_SIZE, 0.0, waitingReducer.outEvent, (applications.at(waitingReducer.appId))->getAmEventType(),
								START_REDUCE_CONTAINER, OTHER, SEIZETOMASTER, waitingReducer.taskLocation);
						eventsList.push(newEvent);
					}
					else{ // map task
						#ifndef TERMINAL_LOG_DISABLED
						std::cout << "NODE||INFO: Create mapper at "<< waitingReducer.taskLocation << " upon releasing a reducer at location: "<< ev->getDestEventType()  << std::endl;
						#endif
						// attribute carries mapperLocation
						Event newEvent(waitingReducer.appId, (ev->getEventTime()+OVERHEAD_DELAY),HEARTBEAT_SIZE, 0.0, waitingReducer.outEvent, (applications.at(waitingReducer.appId))->getAmEventType(),
								START_MAP_CONTAINER, OTHER, SEIZETOMASTER, waitingReducer.taskLocation, waitingReducer.attr, waitingReducer.fsid);
						eventsList.push(newEvent);
					}
				}
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "NODE||INFO: SIMULATION COMPLETED FOR REDUCER ID: " << ev->getRedId() << " OF APPLICATION ID: "<< appID << " time: " << ev->getEventTime() << std::endl;
				#endif
				// SIMULATION FINISHED FOR THIS REDUCER!
				if((applications.at(appID))->checkIfAllReducersComplete()){	// has all reducers been completed?
					#ifndef TERMINAL_LOG_DISABLED
					std::cout<< "---------------------------------------------------------------|\n---------------------------------------------------------------|\n" <<
							"NODE||INFO: OVERALL SIMULATION TIME FOR APPLICATION " << appID << " QueueID: " << (applications.at(appID))->getQueueId() << " Capacity " << g_scheduler.getQueueCapacity((applications.at(appID))->getQueueId()) << " IS " << ev->getEventTime() - (applications.at(appID))->getAppStartTime() << " !" << std::endl;
					#endif

					if(!terminal_output_disabled){
						std::cout<< "---------------------------------------------------------------|\n" <<
													"OVERALL SIMULATION TIME FOR APPLICATION " << appID << " QueueID: " << (applications.at(appID))->getQueueId() << " Capacity " << g_scheduler.getQueueCapacity((applications.at(appID))->getQueueId())
													<< " IS " << ev->getEventTime() - (applications.at(appID))->getAppStartTime() << " !" << std::endl;
					}
					(applications.at(appID))->setReduceFinishTime(ev->getEventTime());

					double mapTime = (applications.at(appID))->getMapFinishTime() - (applications.at(appID))->getMapStartTime();
					double shuffleTime = (applications.at(appID))->getTotalShuffleAvgAmongReducers();
					double avgRedTime =  (applications.at(appID))->avgReduceTime();

					if(avgRedTime < 0.01){
						avgRedTime = 0;
					}

					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "NODE||INFO: MAP SIMULATION TIME FOR APPLICATION " << appID << " IS "<< mapTime <<  std::endl;
					std::cout << "NODE||INFO: AVG REDUCE SIMULATION TIME FOR APPLICATION " << appID << " IS "<<avgRedTime<<  std::endl;
					std::cout << "NODE||INFO: SHUFFLE SIMULATION TIME FOR APPLICATION " << appID << " IS "<< shuffleTime <<  std::endl;
					std::cout << "NODE||INFO: AVERAGE MAP SIMULATION TIME FOR APPLICATION " << appID << " IS "<< (applications.at(appID))->avgMapTime() <<  std::endl;
					#endif

					if(!terminal_output_disabled){
						//std::cout << "REDUCE SIMULATION TIME FOR APPLICATION " << appID << " IS "<< reduceTime <<  std::endl;
						std::cout << "AVERAGE MAP SIMULATION TIME FOR APPLICATION " << appID << " IS "<< (applications.at(appID))->avgMapTime() <<  std::endl;
						std::cout << "AVG REDUCE SIMULATION TIME FOR APPLICATION " << appID << " IS "<<avgRedTime<<  std::endl;
						std::cout << "SHUFFLE SIMULATION TIME FOR APPLICATION " << appID << " IS "<< shuffleTime <<  std::endl;
						std::cout << "MAP SIMULATION TIME FOR APPLICATION " << appID << " IS "<< mapTime <<  std::endl;
					}

					// finally release AM resources
					memory_.setRemainingCapacity(memory_.getRemainingCapacity() + ((applications.at(appID))->getAmResourceMb()*ONE_MB_IN_BYTES));

					g_scheduler.schedulerReleaseAMresources(appID, ev->getEventTime()+OVERHEAD_DELAY);

					#ifdef TESTING_ENABLED
					testing((applications.at(appID))->getMapReduceConfig().getMapreduceJobReduces(), ev->getEventTime(),
							(applications.at(appID))->avgMapTime(), avgRedTime, shuffleTime, mapTime);
					#endif

					// One less application is active at this queue
					currentlyRunningAppCount[applications.at(appID)->getQueueId()]--;

					// Set the queueID_checkPendingApp value (notification for pending apps (if any))
					queueID_checkPendingApp[applications.at(appID)->getQueueId()] = true;
				}
			}
		}
		//-------------------------------------------------------------------------------------DELAY-----------------------------------------------------------------------//
		//-----------------------------------------------------------------------------------------------------------------------------------------------------------------//
		else if(currentEvent == FS_RETRIEVAL){

			// Check for Uber task requirements.
			if(!isUberTask(ev)){

				// for each YARN_NODEMANAGER_HEARTBEAT_INTERVALMS, a heartbeat will be sent to RM and the request for resources for map / reduce tasks will be sent.

				// send Mapper Requests
				int i=0;
				int fileSplitNodeExpectedEventType = (applications.at(appID))->getFileSplitNodeExpectedEventType(i);
				int fsID=0;

				while(fileSplitNodeExpectedEventType != -1){

					// Allocate resources for mapper
					// fileSplitNodeExpectedEventType attribute will be used to provide rack awareness for mapper locations

					Event newEvent(appID, ev->getEventTime()+((i++)*0.0001),HEARTBEAT_SIZE,
							0.0, outputEventType_, (applications.at(appID))->getRmEventType(), ALLOCATE_MAP_RESOURCES_RM, MAPTASK, SEIZETOMASTER, fileSplitNodeExpectedEventType, ev->getFsLoc(), fsID++);

					fileSplitNodeExpectedEventType = (applications.at(appID))->getFileSplitNodeExpectedEventType(i);
					eventsList.push(newEvent);
				}
			}
			else{
				// run tasks in the same JVM as AM
				// TODO: Uber Task Delay! If it is a uber task do not allocate extra resources... Use AM resources.
				printNodeMessages(NODEMANAGER, 5, appID);

				// read filesplits, complete mappers, write intermediate outputs, read intermediate outputs, do reducers, write final output
			}
		}
		else if(currentEvent == NODE_HD_CFQ_EVENT){
			hd_.work(HD_CFQ_EVENT, ev->getSeizedResQuantity(), ev, outputEventType_);
		}
	}
	else{
		// Application run request submitted
		printNodeMessages(OTHERNODE, currentEvent, appID);
	}
}

int Node::getExpectedEventType() const {
	return expectedEventType_;
}

void Node::delay (Event ev){

	int totalCapacity = linkExpEventT_capacity[outputEventType_];
	double tot_delay = APP_RESOURCE_AVG_SIZE/ (double)totalCapacity;

	// Assumed to be 2 hops in the link with outputEventType_
	tot_delay *=2;

	if(ev.getEventBehavior() == LAUNCH_APP_MASTER){
		// delay for "Copy application resources" to HDFS.
		Event newEvent(ev.getAppID(), (ev.getEventTime() + tot_delay), 0.0, 0.0, ev.getDestEventType(), ev.getDestEventType(), INIT_APPLICATION, APPLICATION);
		eventsList.push(newEvent);
	}
	else if(ev.getEventBehavior() == RETRIEVE_SPLITS){
		// delay for "Retrieve Input Splits" from HDFS.
		Event newEvent(ev.getAppID(), (ev.getEventTime() + tot_delay), 0.0, 0.0, ev.getDestEventType(), ev.getDestEventType(), FS_RETRIEVAL, OTHER);
		eventsList.push(newEvent);
	}
}
