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

size_t Node::ID_ = 0;
size_t Node::uniqueID_ = 0;
std::map<int, int> nodeExpEventT_switchExpEventT;				// a mapping for expected event types of node and rack to be used for network simulation
std::map<int, int> nodeExpEventT_linkExpEventT;					// a mapping for expected event types of node and their link to be used for network simulation
std::map<int, remainingCapacities> nodeExpEventT_remainingCap;	// up-to-date remaining capacities
std::map<int, enum NodeType> nodeExpEventT_nodeType;		// used to check if is it a RM node

Node::Node(int rackExpectedEventType, enum NodeType nodeType, int expectedEventType, const Cpu cpu, const Harddisk hd, const Memory memory, int outputEventType):
			Module (expectedEventType, NODE),
			nodeID_(ID_++), rackExpectedEventType_(rackExpectedEventType),
			nodeType_(nodeType), cpu_(cpu),
			hd_(hd), memory_(memory),
			outputEventType_(outputEventType){

	nodeExpEventT_switchExpEventT[expectedEventType]=rackExpectedEventType;
	nodeExpEventT_linkExpEventT[expectedEventType]=outputEventType;

	//memory_.setRemainingCapacity(memory_.getRemainingCapacity()-(2*YARN_HEAPSIZE*ONE_MB_IN_BYTES));		// use memory resources for nodemanager and datanode

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
	 * 	In order to run the application as a uber task, the following should be satisfied:
	 *	mapreduceJobUbertaskEnable: 	true
	 *	mapreduceJobUbertaskMaxmaps:	Threshold for number of maps
	 *	mapreduceJobUbertaskMaxreduces:	Threshold for number of reduces
	 *	mapreduceJobUbertaskMaxbytes:	Threshold for number of input bytes
	 */
	if ((applications.at(ev->getApplicationId()))->getMapReduceConfig().isMapreduceJobUbertaskEnable() &&
			(applications.at(ev->getApplicationId()))->getNumberOfMappers() <= (applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreduceJobUbertaskMaxmaps() &&
			(applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreduceJobReduces() <= (applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreduceJobUbertaskMaxreduces() &&
			(applications.at(ev->getApplicationId()))->getApplicationSize() <= (applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreduceJobUbertaskMaxbytes()
	){
		return true;
	}
	return false;
}

void Node::work (Event* ev){
	// check node type
	#ifndef TERMINAL_LOG_DISABLED
	std::cout<< "TIME: " << ev->getEventTime() << " DESTINATION: " << ev->getDestinationEventType()<< " NODE TYPE " << nodeType_ << " EVENT BEHAVIOR "  << ev->getEventBehavior() <<  std::endl;
	#endif

	if(nodeType_ == CLIENT){
		// check event behavior
		if(ev->getEventBehavior() == SUBMIT_JOB){	/*DOC: "Submit job"*/
			// Application run request submitted
			printNodeMessages(CLIENT, ev->getEventBehavior(), ev->getApplicationId());

			// Request an application ID from Resource Manager (RM event behavior: GET_APP_ID_RM)
			Event newEvent(ev->getApplicationId(), ev->getEventTime(), HEARTBEAT_SIZE, 0.0, outputEventType_, (applications.at(ev->getApplicationId()))->getRmEventType(), GET_APP_ID_RM, APPLICATION);

			eventsList.push(newEvent);
		}
		else if(ev->getEventBehavior() == COPY_APP_RESOURCES){	/*DOC: "Copy application resources"*/
			// Application run request submitted
			printNodeMessages(CLIENT, ev->getEventBehavior(), ev->getApplicationId());

			// Add delay (Write delay to HDFS!)
			delay(*ev);

		}
		else if(ev->getEventBehavior() == 3){

			// Submit application (RM event behavior: SUBMIT_APP_RM)
			Event newEvent(ev->getApplicationId(), ev->getEventTime(), HEARTBEAT_SIZE, 0.0, outputEventType_, (applications.at(ev->getApplicationId()))->getRmEventType(), SUBMIT_APP_RM, APPLICATION);

			eventsList.push(newEvent);

			// Change node type to NODEMANAGER to be able to work with splits...
			nodeType_ = NODEMANAGER;
		}
	}
	else if(nodeType_ == RESOURCEMANAGER){

		if(ev->getEventBehavior() == GET_APP_ID_RM){	/*DOC: "Get Application ID" for Resource Manager*/

			printNodeMessages(RESOURCEMANAGER, ev->getEventBehavior(), ev->getApplicationId());

			// Event to copy application resources (split metadata, app jar, configuration) to HDFS (client event behavior: COPY_APP_RESOURCES)
			Event newEvent(ev->getApplicationId(), (ev->getEventTime() + OVERHEAD_DELAY), HEARTBEAT_SIZE, 0.0, outputEventType_,
					(applications.at(ev->getApplicationId()))->getClientEventType(), COPY_APP_RESOURCES, APPLICATION);

			eventsList.push(newEvent);
		}
		else if(ev->getEventBehavior() == SUBMIT_APP_RM){	/*DOC: "Submit Application" for Resource Manager*/

			printNodeMessages(RESOURCEMANAGER, ev->getEventBehavior(), ev->getApplicationId());

			// Event to start container for a nodemanager (nodemanager event behavior: START_AM_CONTAINER)

			// which NODEMANAGER will be picked? -- Scheduler Decides it based on availablity of resources.
			int nodemanagerEventType = g_scheduler.schedulerSubmitNewApp(ev->getApplicationId());

			(applications.at(ev->getApplicationId()))->setAmEventType(nodemanagerEventType);

			// Event to start container (NODEMANAGER event behavior: START_AM_CONTAINER)
			Event newEvent(ev->getApplicationId(), (ev->getEventTime()+OVERHEAD_DELAY), HEARTBEAT_SIZE, 0.0, outputEventType_, nodemanagerEventType, START_AM_CONTAINER, APPLICATION);

			eventsList.push(newEvent);
		}
		else if(ev->getEventBehavior() == ALLOCATE_RESOURCES_RM){	/*DOC: "Allocate Resources" under Resource Manager (Allocate resources for mapper) */

			printNodeMessages(RESOURCEMANAGER, ev->getEventBehavior(), ev->getApplicationId());
			#ifndef TERMINAL_LOG_DISABLED
						std::cout<< "TIME: " << ev->getEventTime() << " FILESPLIT: " << ev->getEntityIns().getAttribute() << std::endl;
			#endif

			// Scheduler works in here...
			// at which node should the mapper work?
			struct waitingTaskInfo mapperInfo = g_scheduler.schedulerGetMapperLocation(ev->getApplicationId(), ev->getEntityIns().getAttribute(), false,
					outputEventType_, ev->getEntityIns().getAttribute(), ev->getFsId());

			// Mapper Location: mapperInfo.taskLocation (it is -1 in case it is suspended)
			if(mapperInfo.taskLocation == -1){
				#ifndef TERMINAL_LOG_DISABLED
				std::cout<< "Suspended Map Task for application: " << ev->getApplicationId() << std::endl;
				#endif
			}
			else{
				// attribute carries mapperLocation
				Event newEvent(ev->getApplicationId(), (ev->getEventTime()+OVERHEAD_DELAY),HEARTBEAT_SIZE, 0.0, outputEventType_, (applications.at(ev->getApplicationId()))->getAmEventType(),
						START_MAP_CONTAINER, APPLICATION, SEIZETOMASTER, mapperInfo.taskLocation, ev->getEntityIns().getAttribute(), ev->getFsId());
				eventsList.push(newEvent);
			}
		}
		else if(ev->getEventBehavior() == ALLOCATE_RM_RESOURCES_RM){	/*DOC: "Allocate Resources" under Resource Manager (Allocate resources for reducer) */

			printNodeMessages(RESOURCEMANAGER, ev->getEventBehavior(), ev->getApplicationId());

			// Scheduler works in here...
			// at which node should the reducer work?
			struct waitingTaskInfo reducerInfo = g_scheduler.schedulerGetReducerLocation(ev->getApplicationId(), false, outputEventType_);

			// Reducer Location: reducerInfo.taskLocation (it is -1 in case it is suspended)
			if(reducerInfo.taskLocation == -1){
				#ifndef TERMINAL_LOG_DISABLED
				std::cout<< "Suspended Reduce Task for application: " << ev->getApplicationId() << std::endl;
				#endif
			}
			else{
				// Add this reducer location to the list of reducer locations
				(applications.at(ev->getApplicationId()))->addReducerLocations(reducerInfo.taskLocation);

				// attribute carries reducerLocation
				Event newEvent(ev->getApplicationId(), (ev->getEventTime()+OVERHEAD_DELAY), HEARTBEAT_SIZE, 0.0, outputEventType_, (applications.at(ev->getApplicationId()))->getAmEventType(),
						START_REDUCE_CONTAINER, APPLICATION, SEIZETOMASTER, reducerInfo.taskLocation);

				eventsList.push(newEvent);
			}
		}
		else if(ev->getEventBehavior() == RELEASE_REMOTE_FS_READ_RES){	/*DOC: release seized hd read resources in remote node*/
			// release seized hd resources in remote node

			if(ev->getRecordId() == 1){	// there are more packets that were not able to fit in buffer
				// release
				hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

				if ((applications.at(ev->getApplicationId()))->incBufferCompletedPacketCount(ev->getFsId()) == BUFFER_NUMBER_OF_PACKETS){

					// update the remaining number of transfer records
					(applications.at(ev->getApplicationId()))->decrementmapTransferInfoNumberOfMapRecords(ev->getFsId(),BUFFER_NUMBER_OF_PACKETS);

					// get the remaning number of transfer records
					int remaining = (applications.at(ev->getApplicationId()))->getMapTransferInfo_remainingNumberOfMapRecords(ev->getFsId());

					if(remaining > BUFFER_NUMBER_OF_PACKETS){	// not the last set

						ev->setEventBehavior(RECEIVE_FS_TRANSFER_REQUEST);
						ev->setSeizedResQuantity(0);

						for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
							// read filesplit from remote and send a response back to the origin

							// send each record seperately.
							hd_.work(HD_READ_FROM_REMOTE, (applications.at(ev->getApplicationId()))->getRecordSize(), ev, outputEventType_);
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
							// send each record seperately.

							hd_.work(HD_READ_FROM_REMOTE, (applications.at(ev->getApplicationId()))->getRecordSize(), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
						if((applications.at(ev->getApplicationId()))->getMapTransferInfo_lastRecordExist(ev->getFsId())){

							hd_.work(HD_READ_FROM_REMOTE, (applications.at(ev->getApplicationId()))->getMapTransferInfo_lastRecordSize(ev->getFsId()), ev, outputEventType_);
						}
					}
					// reset bufferCompletedPacketCount
					(applications.at(ev->getApplicationId()))->resetBufferCompletedPacketCount(ev->getFsId());
				}
			}
			else{	// the last set of input. just release...
				hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			}
		}
		else if(ev->getEventBehavior() == RECEIVE_FS_TRANSFER_REQUEST){	/*DOC: transfer request is received for a file split*/
			// read filesplit from remote and send read data back to the origin
			// read data from the local disk
			// "read shuffled output" in reducer for each record feed reduce function
			size_t totalSplitSize = (applications.at(ev->getApplicationId()))->getFileSplitSize(ev->getFsId());
			int numberOfMapRecords = totalSplitSize/((applications.at(ev->getApplicationId()))->getRecordSize());

			bool lastRecordExist = false;
			size_t lastRecordSize=0;
			if((size_t)numberOfMapRecords*((applications.at(ev->getApplicationId()))->getRecordSize()) < totalSplitSize){
				lastRecordSize = totalSplitSize - (size_t)numberOfMapRecords*((applications.at(ev->getApplicationId()))->getRecordSize());
				lastRecordExist=true;
			}
			if(lastRecordExist){
				(applications.at(ev->getApplicationId()))->setMapRecordCount(ev->getFsId(), numberOfMapRecords+1);
			}
			else{
				(applications.at(ev->getApplicationId()))->setMapRecordCount(ev->getFsId(), numberOfMapRecords);
			}
			if(numberOfMapRecords > BUFFER_NUMBER_OF_PACKETS){	// enqueue BUFFER_NUMBER_OF_PACKETS packets to the NIC queue

				// set recordId which will be used to determine if there is a need to send more
				// packets affter the releasing resources of BUFFER_NUMBER_OF_PACKETSth packet
				ev->setRecordId(1);

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					// read filesplit from remote and send a response back to the origin
					// send each record seperately.
					hd_.work(HD_READ_FROM_REMOTE, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				// save remaining number of packets to send and the information whether a last packet exists and the lastpacketsize
				(applications.at(ev->getApplicationId()))->setMapTransferInfo(ev->getFsId(), numberOfMapRecords,lastRecordExist,lastRecordSize);
			}
			else{	// send all packets to the NIC queue

				for(int i=0;i<numberOfMapRecords;i++){
					// read filesplit from remote and send a response back to the origin
					// send each record seperately.
					hd_.work(HD_READ_FROM_REMOTE, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				if(lastRecordExist){
					hd_.work(HD_READ_FROM_REMOTE, lastRecordSize, ev, outputEventType_);
				}
			}
		}
		else if(ev->getEventBehavior() == NODE_HD_CFQ_EVENT){
			hd_.work(HD_CFQ_EVENT, ev->getSeizedResQuantity(), ev, outputEventType_);
		}
	}
	else if(nodeType_ == NODEMANAGER){

		if(ev->getEventBehavior() == START_AM_CONTAINER){	/*DOC: eventBehavior: "Start container" under Node Manager*/

			printNodeMessages(NODEMANAGER, ev->getEventBehavior(), ev->getApplicationId());

			// required resources for AM
			size_t amResourcesMB = (applications.at(ev->getApplicationId()))->getAmResourceMb();
			if(g_scheduler.isResourceCalculator()){
				int amCpuVcores = (applications.at(ev->getApplicationId()))->getamCpuVcores();
				// seize resources for AM for CPU
				cpu_.setRemainingNumberOfCores(cpu_.getRemainingNumberOfCores() - amCpuVcores);
			}

			// seize resources for AM for memory
			memory_.setRemainingCapacity(memory_.getRemainingCapacity() - (amResourcesMB*ONE_MB_IN_BYTES));

			// Event to launch application master's process (NODEMANAGER event behavior: LAUNCH_APP_MASTER)
			Event newEvent(ev->getApplicationId(), (ev->getEventTime()+OVERHEAD_DELAY), 0.0, 0.0, ev->getDestinationEventType(), ev->getDestinationEventType(), LAUNCH_APP_MASTER, APPLICATION);

			eventsList.push(newEvent);
		}
		else if(ev->getEventBehavior() == LAUNCH_APP_MASTER){	/*DOC: "Launch Application Master's Process" under Node Manager*/

			printNodeMessages(NODEMANAGER, ev->getEventBehavior(), ev->getApplicationId());

			// Event to Initialize Application (NODEMANAGER event behavior: INIT_APPLICATION)
			Event newEvent(ev->getApplicationId(), (ev->getEventTime()+OVERHEAD_DELAY), 0.0, 0.0, ev->getDestinationEventType(), ev->getDestinationEventType(), INIT_APPLICATION, MAPTASK);

			eventsList.push(newEvent);
		}
		else if(ev->getEventBehavior() == INIT_APPLICATION){	/*DOC: "Initialize Application" under Node Manager*/

			printNodeMessages(NODEMANAGER, ev->getEventBehavior(), ev->getApplicationId());

			// Event to Retrieve Input Splits (NODEMANAGER event behavior: RETRIEVE_SPLITS)
			Event newEvent(ev->getApplicationId(), (ev->getEventTime()+OVERHEAD_DELAY), HEARTBEAT_SIZE, 0.0, ev->getDestinationEventType(), ev->getDestinationEventType(), RETRIEVE_SPLITS, MAPTASK);

			eventsList.push(newEvent);
		}
		else if(ev->getEventBehavior() == RETRIEVE_SPLITS){	/*DOC: "Retrieve Input Splits" under Node Manager*/

			printNodeMessages(NODEMANAGER, ev->getEventBehavior(), ev->getApplicationId());
			delay(*ev);
		}

		else if(ev->getEventBehavior() == START_MAP_CONTAINER){	/*DOC: AM "Start Container" for mapper*/

			printNodeMessages(NODEMANAGER, ev->getEventBehavior(), ev->getApplicationId());
			// follow AM controlled mappers
			(applications.at(ev->getApplicationId()))->pushBackAmControlledMappers(ev->getEntityIns().getAttribute());
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "MAPPER__________________: " << ev->getEntityIns().getAttribute() << std::endl;
			#endif

			(applications.at(ev->getApplicationId()))->addMapStartTime(ev->getFsId(), ev->getEventTime());

			// Event to Launch (NODEMANAGER event behavior: LAUNCH) - attribute LAUNCH_MAP represents this

			if(ev->getDestinationEventType() != ev->getEntityIns().getAttribute()){	// launch at non local
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), HEARTBEAT_SIZE, 0.0, outputEventType_, ev->getEntityIns().getAttribute(), LAUNCH,
						MAPTASK, SEIZETOMASTER, LAUNCH_MAP, ev->getFsLoc(), ev->getFsId());

				eventsList.push(newEvent);
			}
			else{	// launch at local
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), HEARTBEAT_SIZE, 0.0, ev->getEntityIns().getAttribute(), ev->getEntityIns().getAttribute(),
						LAUNCH, MAPTASK, SEIZETOMASTER, LAUNCH_MAP, ev->getFsLoc(), ev->getFsId());

				eventsList.push(newEvent);
			}
		}
		else if(ev->getEventBehavior() == START_REDUCE_CONTAINER){	/*DOC: AM "Start Container" for reducer*/

			printNodeMessages(NODEMANAGER, ev->getEventBehavior(), ev->getApplicationId());

			// follow AM controlled reducers
			(applications.at(ev->getApplicationId()))->pushBackAmControlledReducers(ev->getEntityIns().getAttribute());
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "REDUCER__________________: " << ev->getEntityIns().getAttribute() << std::endl;
			#endif

			// Event to Launch (NODEMANAGER event behavior: LAUNCH) - attribute LAUNCH_REDUCE represents this
			if(ev->getDestinationEventType() != ev->getEntityIns().getAttribute()){	// launch at non local
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), HEARTBEAT_SIZE, 0.0, outputEventType_, ev->getEntityIns().getAttribute(),
						LAUNCH, MAPTASK, SEIZETOMASTER, LAUNCH_REDUCE);

				eventsList.push(newEvent);
			}
			else{	// launch at local
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), HEARTBEAT_SIZE, 0.0, ev->getEntityIns().getAttribute(), ev->getEntityIns().getAttribute(),
						LAUNCH, MAPTASK, SEIZETOMASTER, LAUNCH_REDUCE);

				eventsList.push(newEvent);
			}
		}
		else if(ev->getEventBehavior() == LAUNCH){	/*DOC: "Launch"*/

			printNodeMessages(NODEMANAGER, ev->getEventBehavior(), ev->getApplicationId());
			// Launch delay

			// Event to Retrieve job resources (NODEMANAGER event behavior: RETRIEVE_JOB_RESOURCES)
			Event newEvent(ev->getApplicationId(), (ev->getEventTime() + OVERHEAD_DELAY), 0.0, 0.0, ev->getDestinationEventType(), ev->getDestinationEventType(),
					RETRIEVE_JOB_RESOURCES, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId());

			eventsList.push(newEvent);
		}
		else if(ev->getEventBehavior() == RETRIEVE_JOB_RESOURCES){	/*DOC: "Retrieve job resources"*/

			printNodeMessages(NODEMANAGER, ev->getEventBehavior(), ev->getApplicationId());

			// Note that read and (if required) transfer of filesplit and related delays occurs "after" this step.
			// Event to Run Task (NODEMANAGER event behavior: RUN_TASK)
			Event newEvent(ev->getApplicationId(), ev->getEventTime(), 0.0, 0.0, ev->getDestinationEventType(), ev->getDestinationEventType(), RUN_TASK,
					MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId());

			eventsList.push(newEvent);
		}
		else if(ev->getEventBehavior() == RUN_TASK){	/*DOC: eventBehavior: "Run task"*/

			printNodeMessages(NODEMANAGER, ev->getEventBehavior(), ev->getApplicationId());

			// Event to Run Task (map or reduce)

			// is it a mapper (attribute LAUNCH_MAP: mapper)
			if(ev->getEntityIns().getAttribute() == LAUNCH_MAP){

				if( (applications.at(ev->getApplicationId()))->getMapStartTime() < 0 ){
					(applications.at(ev->getApplicationId()))->setMapStartTime(ev->getEventTime());
				}
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << ">MAPPER ID: " << ev->getFsId() << " START TIME: " << ev->getEventTime() << std::endl;
				#endif

				(applications.at(ev->getApplicationId()))->addMapStartTime(ev->getFsId(), ev->getEventTime());

				// seize resources for Mapper for memory and CPU
				if(g_scheduler.isResourceCalculator()){
					cpu_.setRemainingNumberOfCores(cpu_.getRemainingNumberOfCores() - (applications.at(ev->getApplicationId()))->getSeizedMapCpuVcores());
				}
				memory_.setRemainingCapacity(memory_.getRemainingCapacity() - (applications.at(ev->getApplicationId()))->getSeizedMapreduceMapMemory());

				/*
				 * read the input split (is it local, is it in the same rack or at another rack)
				 * simulate copying data which is saved in HDFS to its local HD
				 * if data is not in local, data request is sent to the worker node that has the data.
				 * when data transfer is complete, the process continues...
				*/
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "STARTED READING FS: " << ev->getFsId() << " at "<< ev->getEventTime() << " located in node: " << ev->getFsLoc() << std::endl;
				#endif

				// local (read)
				if(ev->getFsLoc() == ev->getDestinationEventType()){
					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "READ LOCAL " << ev->getFsId() << std::endl;
					#endif
					// read data from the local disk
					// "read shuffled output" in reducer for each record feed reduce function
					size_t totalSplitSize = (applications.at(ev->getApplicationId()))->getFileSplitSize(ev->getFsId());
					int numberOfMapRecords = totalSplitSize/((applications.at(ev->getApplicationId()))->getRecordSize());

					size_t eachRecordSize=0;
					bool lastRecordExist = false;
					size_t lastRecordSize;
					if((size_t)numberOfMapRecords*((applications.at(ev->getApplicationId()))->getRecordSize()) < totalSplitSize){
						lastRecordSize = totalSplitSize - (size_t)numberOfMapRecords*((applications.at(ev->getApplicationId()))->getRecordSize());
						lastRecordExist=true;
					}
					if(lastRecordExist){

						if(numberOfMapRecords){	// non-zero
							eachRecordSize = ((applications.at(ev->getApplicationId()))->getRecordSize());
						}
						(applications.at(ev->getApplicationId()))->setMapRecordCount(ev->getFsId(), numberOfMapRecords+1);
					}
					else{
						eachRecordSize = totalSplitSize/(numberOfMapRecords);
						(applications.at(ev->getApplicationId()))->setMapRecordCount(ev->getFsId(), numberOfMapRecords);
					}
					(applications.at(ev->getApplicationId()))->setRecordInfo(ev->getFsId(), eachRecordSize, lastRecordSize, lastRecordExist, numberOfMapRecords, true);

					// create a new event for the first record to be read at HD
					Event newEvent(ev->getApplicationId(), ev->getEventTime() + (3*YARN_NODEMANAGER_HEARTBEAT_INTERVALMS)/1000, -1, -1, ev->getDestinationEventType(), ev->getDestinationEventType(),
							READ_NEW_RECORD, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), -1, BUFFER_NUMBER_OF_PACKETS);

					eventsList.push(newEvent);
				}
				// same rack (transfer & read)
				else if(nodeExpEventT_switchExpEventT[ev->getFsLoc()] == nodeExpEventT_switchExpEventT[ev->getDestinationEventType()]){
					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "READ SAME RACK " << ev->getFsId()  << std::endl;
					#endif
					// send transfer request to file split location (NODEMANAGER event behavior: RECEIVE_FS_TRANSFER_REQUEST)
					// save return address in "attribute"
					Event newEvent(ev->getApplicationId(), ev->getEventTime() + (3*YARN_NODEMANAGER_HEARTBEAT_INTERVALMS)/1000, HEARTBEAT_SIZE, 0.0, outputEventType_, ev->getFsLoc(),
							RECEIVE_FS_TRANSFER_REQUEST, MAPTASK, SEIZETOMASTER, ev->getDestinationEventType(), ev->getFsLoc(), ev->getFsId());

					eventsList.push(newEvent);
				}
				// somewhere in the cluster (transfer & read)
				else{
					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "READ ANOTHER RACK " << ev->getFsId()  << std::endl;
					#endif
					// send transfer request to file split location (NODEMANAGER event behavior: RECEIVE_FS_TRANSFER_REQUEST)
					// save return address in "attribute"
					Event newEvent(ev->getApplicationId(), ev->getEventTime() + (3*YARN_NODEMANAGER_HEARTBEAT_INTERVALMS)/1000, HEARTBEAT_SIZE, 0.0, outputEventType_, ev->getFsLoc(),
							RECEIVE_FS_TRANSFER_REQUEST, MAPTASK, SEIZETOMASTER, ev->getDestinationEventType(), ev->getFsLoc(), ev->getFsId());

					eventsList.push(newEvent);
				}
			}
			// is it a reducer (attribute LAUNCH_REDUCE: reduce task)
			else if(ev->getEntityIns().getAttribute() == LAUNCH_REDUCE){

				// seize resources for Reducer for memory and CPU
				if(g_scheduler.isResourceCalculator()){
					cpu_.setRemainingNumberOfCores(cpu_.getRemainingNumberOfCores() - (applications.at(ev->getApplicationId()))->getSeizedReduceCpuVcores());
				}
				memory_.setRemainingCapacity(memory_.getRemainingCapacity() - (applications.at(ev->getApplicationId()))->getSeizedMapreduceReduceMemory());
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "REDUCER______________------------- " <<  ev->getDestinationEventType() << std::endl;
				#endif

				if((applications.at(ev->getApplicationId()))->isReducerCompletionWaitersHasElement()){
					// then there are some map output waiting to be shuffled to reducers

					size_t totalwaiters= (applications.at(ev->getApplicationId()))->gettotalReducerCompletionWaiters();
					for(size_t i=0; i < totalwaiters;i++){
						#ifndef TERMINAL_LOG_DISABLED
						std::cout << "SHUFFLE ALL"  <<  std::endl;
						#endif
						reducerWaiterData waiterData = (applications.at(ev->getApplicationId()))->getReducerCompletionWaiter(i);

						int finalDest = (applications.at(ev->getApplicationId()))->getReducerLocation(waiterData.attribute_);

						if(finalDest != -1){	// reducer location found (which means reducer was created) and data was not shuffled (since it was waiting in waiterData)

							// Shuffle start time
							if((applications.at(ev->getApplicationId()))->getShuffleStartTime(waiterData.attribute_) < 0){
								(applications.at(ev->getApplicationId()))->setShuffleStartTime(ev->getEventTime(), waiterData.attribute_);
							}
							// add shuffle start time
							(applications.at(ev->getApplicationId()))->addShuffleStartTime(waiterData.fsID_, waiterData.attribute_, ev->getEventTime());
							// read data at corresponding node to be sent to a reducer (waiterData.attribute_ will be used to get reducer location)
							Event newEvent(ev->getApplicationId(), ev->getEventTime(), waiterData.neededResQuantity_, 0, waiterData.myLoc_, waiterData.myLoc_,
									SHUFFLE_READ_DATA, REDUCETASK, SEIZETOMASTER, waiterData.attribute_, -1, waiterData.fsID_, waiterData.attribute_);
							eventsList.push(newEvent);

							// data shuffle started so set shuffle flag to true
							(applications.at(ev->getApplicationId()))->signalReducerCompletionWaiter(i);
						}
					}
					// clear shuffle started elements from reducer completion waiter list (those who wait fro reducer to be created to start shuffle...)
					(applications.at(ev->getApplicationId()))->clearFinishedReducerCompletionWaiters();
				}
			}
			else{
				std::cerr << "Unrecognized task type: " << ev->getApplicationId() << std::endl;
			}
		}

		else if(ev->getEventBehavior() == READ_NEW_RECORD){		// read one more record for mapper function

			recordInfo newRecord = (applications.at(ev->getApplicationId()))->getRecordInfo(ev->getFsId());

			if(newRecord.remainingRecordCount_ > BUFFER_NUMBER_OF_PACKETS){	// send BUFFER_NUMBER_OF_PACKETS now. upon completion send more...

				// decrement remaining map record count
				(applications.at(ev->getApplicationId()))->decrementRecordInfoRemainingMapRecordCountAmount(ev->getFsId(), BUFFER_NUMBER_OF_PACKETS);

				ev->setRecordId(newRecord.remainingRecordCount_);

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					// read each record and upon completion release the seized resource and start map function on the record just read

					hd_.work(HD_READ_FOR_MAP_TASK, newRecord.eachRecordSize_, ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
			else{	// send all the remaining
				// reset remaining map record count
				(applications.at(ev->getApplicationId()))->decrementRecordInfoRemainingMapRecordCountAmount(ev->getFsId(), newRecord.remainingRecordCount_);

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

		else if(ev->getEventBehavior() == FS_RECORD_READ_FINISH){	/*DOC: completion of filesplit record read in the mapper. now the mapper is ready to start map function and corresponding hd resource will be released */
			// release disk resource
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			// done reading! continue with next step (map function).
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "FS SIZE: " <<  ev->getNeededResQuantity()<< " fsID " << ev->getFsId() <<  std::endl; // process this data in mapper cpu
			#endif
			// At this point check if map function is "unit" function... in case of unit function, transient data was never spilled to disk other at the end of the map.

			Event newEvent(ev->getApplicationId(), ev->getEventTime(), ev->getNeededResQuantity(), ev->getSeizedResQuantity(), ev->getNextEventType(), ev->getDestinationEventType(),
					RUN_MAP_FUNCTION, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRecordId());

			eventsList.push(newEvent);
		}
		else if(ev->getEventBehavior() == RELEASE_REMOTE_FS_READ_RES){	/*DOC: release seized hd read resources in remote node*/
			// release seized hd resources in remote node
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			if(ev->getRecordId() == 1 &&
					(applications.at(ev->getApplicationId()))->incBufferCompletedPacketCount(ev->getFsId()) == BUFFER_NUMBER_OF_PACKETS
					){	// there are more packets that were not able to fit in buffer

				// update the remaining number of transfer records
				(applications.at(ev->getApplicationId()))->decrementmapTransferInfoNumberOfMapRecords(ev->getFsId(),BUFFER_NUMBER_OF_PACKETS);

				// get the remaning number of transfer records
				int remaining = (applications.at(ev->getApplicationId()))->getMapTransferInfo_remainingNumberOfMapRecords(ev->getFsId());

				if(remaining > BUFFER_NUMBER_OF_PACKETS){	// not the last set

					ev->setEventBehavior(RECEIVE_FS_TRANSFER_REQUEST);
					ev->setSeizedResQuantity(0);

					for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
						// read filesplit from remote and send a response back to the origin

						// send each record seperately.
						hd_.work(HD_READ_FROM_REMOTE, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
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
						// send each record seperately.

						hd_.work(HD_READ_FROM_REMOTE, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
						ev->incGlobalEntityId();
					}
					if((applications.at(ev->getApplicationId()))->getMapTransferInfo_lastRecordExist(ev->getFsId())){
						hd_.work(HD_READ_FROM_REMOTE, (applications.at(ev->getApplicationId()))->getMapTransferInfo_lastRecordSize(ev->getFsId()), ev, outputEventType_);
					}
				}
				// reset bufferCompletedPacketCount
				(applications.at(ev->getApplicationId()))->resetBufferCompletedPacketCount(ev->getFsId());
			}
		}

		else if(ev->getEventBehavior() == RECEIVE_FS_TRANSFER_REQUEST){	/*DOC: transfer request is received for a file split*/
			// read filesplit from remote and send read data back to the origin
			// read data from the local disk
			// "read shuffled output" in reducer for each record feed reduce function
			size_t totalSplitSize = (applications.at(ev->getApplicationId()))->getFileSplitSize(ev->getFsId());
			int numberOfMapRecords = totalSplitSize/((applications.at(ev->getApplicationId()))->getRecordSize());
			bool lastRecordExist = false;
			size_t lastRecordSize=0;
			if((size_t)numberOfMapRecords*((applications.at(ev->getApplicationId()))->getRecordSize()) < totalSplitSize){
				lastRecordSize = totalSplitSize - (size_t)numberOfMapRecords*((applications.at(ev->getApplicationId()))->getRecordSize());
				lastRecordExist=true;
			}
			if(lastRecordExist){
				(applications.at(ev->getApplicationId()))->setMapRecordCount(ev->getFsId(), numberOfMapRecords+1);
			}
			else{
				(applications.at(ev->getApplicationId()))->setMapRecordCount(ev->getFsId(), numberOfMapRecords);
			}
			if(numberOfMapRecords > BUFFER_NUMBER_OF_PACKETS){	// enqueue BUFFER_NUMBER_OF_PACKETS packets to the NIC queue

				// set recordId which will be used to determine if there is a need to send more
				// packets affter the releasing resources of BUFFER_NUMBER_OF_PACKETSth packet
				ev->setRecordId(1);

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					// read filesplit from remote and send a response back to the origin
					// send each record seperately.
					hd_.work(HD_READ_FROM_REMOTE, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				// save remaining number of packets to send and the information whether a last packet exists and the lastpacketsize
				(applications.at(ev->getApplicationId()))->setMapTransferInfo(ev->getFsId(), numberOfMapRecords,lastRecordExist,lastRecordSize);
			}
			else{	// send all packets to the NIC queue

				for(int i=0;i<numberOfMapRecords;i++){
					// read filesplit from remote and send a response back to the origin
					// send each record seperately.
					hd_.work(HD_READ_FROM_REMOTE, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				if(lastRecordExist){
					hd_.work(HD_READ_FROM_REMOTE, lastRecordSize, ev, outputEventType_);
				}
			}
		}
		else if(ev->getEventBehavior() == WRITE_TRANSFERRED_FS_TO_LOCAL){	/*DOC: transfer from remote node is complete. now write the transferred filesplit*/
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "RECEIVED FS: " << ev->getFsId() << " at "<< ev->getEventTime() << " located in node: " << ev->getFsLoc() << std::endl;
			#endif

			// write transferred data to the local disk
			hd_.work(HD_WRITE_TO_LOCAL, ev->getNeededResQuantity(), ev, outputEventType_);
		}
		else if(ev->getEventBehavior() == RELEASE_FS_TRANSFERRED_WRITE_RES){	/*DOC: release seized hd write resources in local node*/
			// release disk resource and check waiting queue for other write requests for mapper
			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			// read transferred data from the local disk

			// create a new event for last record to be read at HD
			Event newEvent(ev->getApplicationId(), ev->getEventTime(), ev->getNeededResQuantity(), -1, ev->getDestinationEventType(), ev->getDestinationEventType(),
					-1, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), -1);

			// read each record and upon completion release the seized resource and start map function on the record just read

			hd_.work(HD_READ_FOR_MAP_TASK, ev->getNeededResQuantity(), &newEvent, outputEventType_);
		}
		else if(ev->getEventBehavior() == RUN_MAP_FUNCTION){	/*DOC: Map function */
			// execute map function

			// note that in any case seized_mapCpuVcores will be fetched (even if the default resource calculator is used)
			int seized_mapCpuVcores= (applications.at(ev->getApplicationId()))->getSeizedMapCpuVcores();
			// calculate spillLimitInBytes
			double sortSpillPercent = (applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreduceMapSortSpillPercent();
			size_t mapreduceTaskIOSortMb = (applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreduceTaskIoSortMb();
			size_t spillLimitInBytes = sortSpillPercent * (mapreduceTaskIOSortMb  << 20);
			double effectiveCoreCountPerMapTask;

			if ( activeMappersInThisNode_.find(ev->getFsId()) == activeMappersInThisNode_.end() ) {	// not found
				activeMappersInThisNode_[ev->getFsId()] = '1';
			}

			int mapCapacityOfThisNode = g_scheduler.getnodeId_maxNumberOfMapTaskPerNode(ev->getDestinationEventType());
			int freeSlotsInThisNode = mapCapacityOfThisNode - activeMappersInThisNode_.size();
			int remainingNumberOfMapTasks = (applications.at(ev->getApplicationId()))->getTotalNumberOfMappers() - (applications.at(ev->getApplicationId()))->getCompletedMapperCount();

			if(remainingNumberOfMapTasks  < freeSlotsInThisNode){
				effectiveCoreCountPerMapTask = (freeSlotsInThisNode/(double)remainingNumberOfMapTasks)*cpu_.getRemainingNumberOfCores()/((double)(mapCapacityOfThisNode));
			}
			else{
				effectiveCoreCountPerMapTask = cpu_.getRemainingNumberOfCores()/((double)(mapCapacityOfThisNode));
			}

			if (seized_mapCpuVcores < effectiveCoreCountPerMapTask){
				effectiveCoreCountPerMapTask = seized_mapCpuVcores;
			}
			cpu_.setEffectiveMapCoreCapacity(effectiveCoreCountPerMapTask);

			// spills will create a map sort event (Event: RUN_MAP_SORT)
			cpu_.mapFunction(ev, spillLimitInBytes, (mapreduceTaskIOSortMb  << 20), seized_mapCpuVcores, (applications.at(ev->getApplicationId()))->getMapIntensity(),
					(applications.at(ev->getApplicationId()))->getMapRecordCount(ev->getFsId()));
		}
		// partition sort...
		else if(ev->getEventBehavior() == RUN_MAP_SORT){	/*DOC: map sort function */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++SORT & PARTITION+++++++++++++++++++TIME:   " << ev->getEventTime() << " fsid: " << ev->getFsId() << " sort ev->getNeededResQuantity() " << ev->getNeededResQuantity() << std::endl;
			#endif
			if(ev->getRedId() == NON_SPILL){	// NON_SPILL
				cpu_.nonSpillComplete(ev->getFsId(), ev->getNeededResQuantity(), ev);
			}
			else{	// SPILL
				if( (applications.at(ev->getApplicationId()))->isThereACombiner() ){	// there is a combiner function
					cpu_.sortWork(ev->getNeededResQuantity(), ev->getEntityIns().getAttribute(),ev, (applications.at(ev->getApplicationId()))->getMapIntensity(), true);
				}
				else{
					// set recordID to MAP_SORT_SOURCE to signal that the data flow is coming from a map sort
					ev->setRecordId(MAP_SORT_SOURCE);
					cpu_.sortWork(ev->getNeededResQuantity(), ev->getEntityIns().getAttribute(),ev, (applications.at(ev->getApplicationId()))->getMapIntensity());
				}
			}
		}
		else if(ev->getEventBehavior() == RUN_COMBINER){	/*DOC: combiner */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++COMBINER+++++++++++++++++++TIME:   " << ev->getEventTime() << " ev->getNeededResQuantity() " << ev->getNeededResQuantity() << " fsID " << ev->getFsId() << std::endl;
			#endif
			cpu_.decCpuUserCount(ev->getFsId());
			// set recordID to COMBINER_SOURCE to signal that the data flow is coming from a combiner
			ev->setRecordId(COMBINER_SOURCE);

			// set the spill amount so that when the write to disk is completed, it can be cleared from the buffer of map task
			ev->setSpillTally(ev->getNeededResQuantity());

			double compressionRatio = (applications.at(ev->getApplicationId()))->getMapOutputVolume() * 0.01;
			cpu_.combinerWork(ev->getNeededResQuantity()*compressionRatio, ev->getEntityIns().getAttribute(), ev,
					(applications.at(ev->getApplicationId()))->getCombinerIntensity());
		}

		// spill...
		else if(ev->getEventBehavior() == RUN_MAP_SPILL){	/*DOC: map spill function */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++SPILL+++++++++++++++++++TIME:   " << ev->getEventTime() << " ev->getNeededResQuantity() " << ev->getNeededResQuantity() << " fsID " << ev->getFsId() << std::endl;
			#endif
			cpu_.decCpuUserCount(ev->getFsId());
			// Now do the spill work...
			// write data to the local disk (spill)

			if( ev->getRecordId() == COMBINER_SOURCE){	// there was a combiner (combiner related compression is applied)
				hd_.work(HD_WRITE_TRANSFERRED_TO_LOCAL, ev->getNeededResQuantity()* ((applications.at(ev->getApplicationId()))->getCombinerCompressionPercent() * 0.01), ev, outputEventType_);
			}
			else{	// no combiner (map output compression is applied)

				// set the spill amount so that when the write to disk is completed, it can be cleared from the buffer of map task
				ev->setSpillTally(ev->getNeededResQuantity());
				hd_.work(HD_WRITE_TRANSFERRED_TO_LOCAL, ev->getNeededResQuantity()* ((applications.at(ev->getApplicationId()))->getMapOutputVolume() * 0.01), ev, outputEventType_);
			}
		}
		else if(ev->getEventBehavior() == MAP_REMAINING_WORK){	/*DOC: Map (check CPU suspend requirement)*/
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++CPU SUSPEND CHECK++++++++++++++TIME:   " << ev->getEventTime() << "FsId: " << ev->getFsId() << std::endl;
			#endif
			double sortSpillPercent = (applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreduceMapSortSpillPercent();

			cpu_.remainingMapWork(ev->getNeededResQuantity(), ev->getEntityIns().getAttribute(), ev, ev->getSeizedResQuantity(), (size_t)(ev->getSeizedResQuantity() * sortSpillPercent),
					(applications.at(ev->getApplicationId()))->getMapIntensity() );
		}
		else if(ev->getEventBehavior() == MAP_MERGE_READY){	/*DOC: mapper (merge ready) preceded by map spill (RUN_MAP_SPILL)*/
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++MERGE READY+++++++++++++++++++TIME:   " << ev->getEventTime() << " fsID " << ev->getFsId() << " seized: "<< ev->getSeizedResQuantity() << std::endl;
			#endif

			// release disk resource for spill write
			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			// notify spill completion and initiate other spills if needed
			bool allSpillsCompleted = cpu_.mapSpillCompleted(ev->getFsId(), ev->getSpillTally(), ev);
			// add map merge ready size
			(applications.at(ev->getApplicationId()))->addMapMergeReadySize(ev->getFsId(), ev->getNeededResQuantity());

			if(allSpillsCompleted){

				size_t mapperOutputSize = (applications.at(ev->getApplicationId()))->getMapMergeReadySize(ev->getFsId());
				(applications.at(ev->getApplicationId()))->addMergeStartTime(ev->getFsId(), ev->getEventTime());

				#ifndef TERMINAL_LOG_DISABLED
				std::cout<< "START MERGING FsID: "<< ev->getFsId() << " in Mapper for application: " << ev->getApplicationId() << " mapperOutputSize " << mapperOutputSize <<  " time: " << ev->getEventTime() << std::endl;
				#endif
				int totalReducerCount = (applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreduceJobReduces();
				double parititonOfEachReducer = mapperOutputSize/(double)totalReducerCount;

				ev->setSeizedResQuantity(0);
				ev->setNextEventType(ev->getDestinationEventType());

				int numberOfMergeRecords = parititonOfEachReducer/((applications.at(ev->getApplicationId()))->getRecordSize());
				(applications.at(ev->getApplicationId()))->setReducerPartitionSize(ev->getFsId(), parititonOfEachReducer);

				bool lastRecordExist = false;
				double lastRecordSize=0;
				if((size_t)numberOfMergeRecords*((applications.at(ev->getApplicationId()))->getRecordSize()) < parititonOfEachReducer){
					lastRecordSize = parititonOfEachReducer - (size_t)numberOfMergeRecords*((applications.at(ev->getApplicationId()))->getRecordSize());
					lastRecordExist=true;
				}

				if(lastRecordExist){
					(applications.at(ev->getApplicationId()))->setMapMergeRecordCount(ev->getFsId(), numberOfMergeRecords+1);
				}
				else{
					(applications.at(ev->getApplicationId()))->setMapMergeRecordCount(ev->getFsId(), numberOfMergeRecords);
				}

				if(numberOfMergeRecords > BUFFER_NUMBER_OF_PACKETS){
					ev->setRecordId(1);
				}
				else{
					ev->setRecordId(-1);
				}
				for(int i=0;i<totalReducerCount;i++)
				{
					// use redID to determine the merge count
					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "Start Merge: " << ev->getFsId() << " totalReducerCount " << totalReducerCount<<  " time: " << ev->getEventTime() << " mapperOutputSize: " << mapperOutputSize/totalReducerCount << " redId " << i << std::endl;
					#endif
					// merge partitions for each reducer
					ev->setRedId(i);

					// but read each record separately
					if(numberOfMergeRecords > BUFFER_NUMBER_OF_PACKETS){	// enqueue BUFFER_NUMBER_OF_PACKETS packets to the NIC queue

						// set recordId which will be used to determine if there is a need to read more
						// packets after the releasing resources of BUFFER_NUMBER_OF_PACKETSth packet

						for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
							// read each record seperately.
							hd_.work(HD_READ_MAP_OUTPUT, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
					}
					else{	// send all packets to the NIC queue

						for(int i=0;i<numberOfMergeRecords;i++){
							// read each record seperately.
							hd_.work(HD_READ_MAP_OUTPUT, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
						if(lastRecordExist){
							hd_.work(HD_READ_MAP_OUTPUT, lastRecordSize, ev, outputEventType_);
							ev->incGlobalEntityId();
						}
					}
					// save remaining number of packets to send and the information whether a last packet exists and the lastpacketsize
					if(lastRecordExist){
						(applications.at(ev->getApplicationId()))->setMapMergeInfo(ev->getFsId(), ev->getRedId(), numberOfMergeRecords+1, lastRecordExist, lastRecordSize);
					}
					else{
						(applications.at(ev->getApplicationId()))->setMapMergeInfo(ev->getFsId(), ev->getRedId(), numberOfMergeRecords, lastRecordExist, lastRecordSize);
					}
				}
			}
		}
		else if(ev->getEventBehavior() == MERGE_PARTITIONS_FOR_REDUCER){ /*DOC: merge partitions for each reducer*/
			// release disk resource and check waiting queue for read request for mapper
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			// release seized hd resources in remote node
			if(ev->getRecordId() == 1){	// there are more packets that were not able to fit in buffer

				if ((applications.at(ev->getApplicationId()))->incMergeBufferCompletedPacketCount(ev->getFsId(), ev->getRedId()) == BUFFER_NUMBER_OF_PACKETS){

					// update the remaining number of transfer records
					(applications.at(ev->getApplicationId()))->decrementMergeInfoNumberOfMapRecords(ev->getFsId(), ev->getRedId(),BUFFER_NUMBER_OF_PACKETS);

					// get the remaning number of transfer records
					int remaining = (applications.at(ev->getApplicationId()))->getMergeInfo_remainingNumberOfMapRecords(ev->getFsId(), ev->getRedId());

					if(remaining > BUFFER_NUMBER_OF_PACKETS){	// not the last set

						ev->setSeizedResQuantity(0);

						for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
							// read filesplit from remote and send a response back to the origin

							// send each record seperately.
							hd_.work(HD_READ_MAP_OUTPUT, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
					}
					else{	// it is the last set
						// last set of records!
						ev->setRecordId(-1);
						ev->setSeizedResQuantity(0);

						for(int i=0;i<remaining;i++){
							// read filesplit from remote and send a response back to the origin
							// send each record seperately.
							hd_.work(HD_READ_MAP_OUTPUT, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
						if((applications.at(ev->getApplicationId()))->getMergeInfo_lastRecordExist(ev->getFsId(), ev->getRedId())){
							hd_.work(HD_READ_MAP_OUTPUT, (applications.at(ev->getApplicationId()))->getMergeInfo_lastRecordSize(ev->getFsId(), ev->getRedId()), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
					}
					// reset bufferCompletedPacketCount
					(applications.at(ev->getApplicationId()))->resetMergeBufferCompletedPacketCount(ev->getFsId(), ev->getRedId());
				}
			}
			else{	// the last set of input. just release...

				if(!(applications.at(ev->getApplicationId()))->decrementMergeInfoNumberOfMapRecords(ev->getFsId(), ev->getRedId(),1)){	// done

					// start processing the data (merge)
					// create an event to call event START_REDUCE_SORT_FUNC

					Event newEvent(ev->getApplicationId(), ev->getEventTime(), (applications.at(ev->getApplicationId()))->getReducerPartitionSize(ev->getFsId()), 0, ev->getDestinationEventType(), ev->getDestinationEventType(),
							CPU_MAP_MERGE, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());

					eventsList.push(newEvent);
				}
			}
		}
		else if(ev->getEventBehavior() == CPU_MAP_MERGE){	/*DOC: mapper merge in cpu*/
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++MERGE IN CPU+++++++++++++++++++TIME:   " << ev->getEventTime() << "attribute: " << ev->getEntityIns().getAttribute() <<  " fsID " << ev->getFsId() <<
					" --needed " << ev->getNeededResQuantity() <<  " seized: "<< ev->getSeizedResQuantity() << std::endl;
			std::cout << "Merge Read Complete: " << ev->getFsId() <<  "  " << ev->getEventTime() << " redId " << ev->getRedId()<< std::endl;
			#endif

			// start measuring CPU merge function time.
			(applications.at(ev->getApplicationId()))->addMergeFnStartTime(ev->getFsId(), ev->getEventTime());
			cpu_.mergeWork(ev->getNeededResQuantity(), ev->getEntityIns().getAttribute(),ev, 2*(applications.at(ev->getApplicationId()))->getMapIntensity());
		}
		else if(ev->getEventBehavior() == MAP_MERGE_WB){	/*DOC: mapper write back */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++MERGE IN WRITEBACK+++++++++++++++++++TIME:   " << ev->getEventTime() << " fsID " << ev->getFsId() << " seized: "<< ev->getSeizedResQuantity() <<
					" @needed: " << ev->getNeededResQuantity() << std::endl;
			std::cout << "Merge CPU Process Complete: " << ev->getFsId() <<  "  " << ev->getEventTime() << "for reducer " << ev->getRedId() <<  std::endl;
			#endif
			cpu_.decCpuUserCount(ev->getFsId());

			// delete from the map of active maps in this node
			if ( activeMappersInThisNode_.find(ev->getFsId()) != activeMappersInThisNode_.end() ) {	// found
				activeMappersInThisNode_.erase(ev->getFsId());
				(applications.at(ev->getApplicationId()))->incCompletedMapperCount();
			}
			// again here write each part instead of writing all at once...
			double parititonOfEachReducer = (applications.at(ev->getApplicationId()))->getReducerPartitionSize(ev->getFsId());

			ev->setSeizedResQuantity(0);

			int numberOfMergeRecords = parititonOfEachReducer/((applications.at(ev->getApplicationId()))->getRecordSize());

			bool lastRecordExist = false;
			double lastRecordSize=0;
			if((size_t)numberOfMergeRecords*((applications.at(ev->getApplicationId()))->getRecordSize()) < parititonOfEachReducer){
				lastRecordSize = parititonOfEachReducer - (size_t)numberOfMergeRecords*((applications.at(ev->getApplicationId()))->getRecordSize());
				lastRecordExist=true;
			}
			if(lastRecordExist){
				(applications.at(ev->getApplicationId()))->setMapMergeRecordCount(ev->getFsId(), numberOfMergeRecords+1);
			}
			else{
				(applications.at(ev->getApplicationId()))->setMapMergeRecordCount(ev->getFsId(), numberOfMergeRecords);
			}

			// set recordId which will be used to determine if there is a need to read more
			// packets after the releasing resources of BUFFER_NUMBER_OF_PACKETSth packet
			if(numberOfMergeRecords > BUFFER_NUMBER_OF_PACKETS){
				ev->setRecordId(1);
			}
			else{
				ev->setRecordId(-1);
			}

			if(numberOfMergeRecords > BUFFER_NUMBER_OF_PACKETS){

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					// write each record seperately.
					hd_.work(HD_WRITE_MERGED_DATA, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
			else{

				for(int i=0;i<numberOfMergeRecords;i++){
					// write each record seperately.
					hd_.work(HD_WRITE_MERGED_DATA, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				if(lastRecordExist){
					hd_.work(HD_WRITE_MERGED_DATA, lastRecordSize, ev, outputEventType_);
				}
			}
			// save remaining number of packets to send and the information whether a last packet exists and the lastpacketsize
			if(lastRecordExist){
				(applications.at(ev->getApplicationId()))->setMapMergeInfo(ev->getFsId(), ev->getRedId(), numberOfMergeRecords+1, lastRecordExist, lastRecordSize);
			}
			else{
				(applications.at(ev->getApplicationId()))->setMapMergeInfo(ev->getFsId(), ev->getRedId(), numberOfMergeRecords, lastRecordExist, lastRecordSize);
			}

		}
		else if(ev->getEventBehavior() == MAP_MERGE_WB_COMPLETE){	/*DOC: mapper write back completed release resources (ready for reducer(s))*/
			// release disk resource and check waiting queue for write request
			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			bool donewriting = false;

			if(ev->getRecordId() == 1){	// there are more packets that were not able to fit in buffer

				if ((applications.at(ev->getApplicationId()))->incMergeBufferCompletedPacketCount(ev->getFsId(), ev->getRedId()) == BUFFER_NUMBER_OF_PACKETS){

					// update the remaining number of transfer records
					(applications.at(ev->getApplicationId()))->decrementMergeInfoNumberOfMapRecords(ev->getFsId(), ev->getRedId(),BUFFER_NUMBER_OF_PACKETS);

					// get the remaning number of transfer records
					int remaining = (applications.at(ev->getApplicationId()))->getMergeInfo_remainingNumberOfMapRecords(ev->getFsId(), ev->getRedId());

					if(remaining > BUFFER_NUMBER_OF_PACKETS){	// not the last set

						ev->setSeizedResQuantity(0);

						for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
							// read filesplit from remote and send a response back to the origin

							// send each record seperately.
							hd_.work(HD_WRITE_MERGED_DATA, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
					}
					else{	// it is the last set
						// last set of records!
						ev->setRecordId(-1);
						ev->setSeizedResQuantity(0);

						for(int i=0;i<remaining;i++){
							// read filesplit from remote and send a response back to the origin
							// send each record seperately.

							hd_.work(HD_WRITE_MERGED_DATA, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
							ev->incGlobalEntityId();
						}
						if((applications.at(ev->getApplicationId()))->getMergeInfo_lastRecordExist(ev->getFsId(), ev->getRedId())){

							hd_.work(HD_WRITE_MERGED_DATA, (applications.at(ev->getApplicationId()))->getMergeInfo_lastRecordSize(ev->getFsId(), ev->getRedId()), ev, outputEventType_);
						}
					}
					// reset bufferCompletedPacketCount
					(applications.at(ev->getApplicationId()))->resetMergeBufferCompletedPacketCount(ev->getFsId(), ev->getRedId());
				}
			}
			// the last set of input. just release...
			else if(!(applications.at(ev->getApplicationId()))->decrementMergeInfoNumberOfMapRecords(ev->getFsId(), ev->getRedId(),1)){	// done
				// done writing
				donewriting = true;
				ev->setNeededResQuantity((applications.at(ev->getApplicationId()))->getReducerPartitionSize(ev->getFsId()));
			}
			// All the parts of reduce partition with this fsid is written after this point

			if( donewriting && (applications.at(ev->getApplicationId()))->notifyMapMergeComplete(ev->getFsId())  ){	// all partitions for each reducer are merged

				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "+++++++MERGE WRITEBACK COMPLETE+++++++++++++TIME:   " << ev->getEventTime() << " fsID " << ev->getFsId() << " seized: "<< ev->getSeizedResQuantity() << std::endl;
				std::cout << "~Mapper ID: " << ev->getFsId() << " finish time: " << ev->getEventTime() << std::endl;
				#endif

				(applications.at(ev->getApplicationId()))->addMapFinishTime(ev->getFsId(), ev->getEventTime());
				(applications.at(ev->getApplicationId()))->notifyWbCompleteMappers();

				// add created data to the list of completed mappers
				(applications.at(ev->getApplicationId()))->saveReadyToShuffleInfo(ev->getFsId(), ev->getDestinationEventType(), ev->getNeededResQuantity());

				// READY FOR REDUCER PHASE!
				// release mapper resources
				memory_.setRemainingCapacity(memory_.getRemainingCapacity() + (applications.at(ev->getApplicationId()))->getSeizedMapreduceMapMemory());
				struct waitingTaskInfo waitingMapper = g_scheduler.schedulerReleaseMapperresources(ev->getApplicationId(), ev->getDestinationEventType(), ev->getFsId());

				if(waitingMapper.taskLocation != -2){

					if(waitingMapper.taskType){	// reducer
						#ifndef TERMINAL_LOG_DISABLED
						std::cout << "Create reducer at "<< waitingMapper.taskLocation << " upon releasing a mapper at location: "<< ev->getDestinationEventType()  << std::endl;
						#endif

						(applications.at(ev->getApplicationId()))->addReducerLocations(waitingMapper.taskLocation);

						Event newEvent(waitingMapper.appId, (ev->getEventTime()+OVERHEAD_DELAY),HEARTBEAT_SIZE, 0.0, waitingMapper.outEvent, (applications.at(waitingMapper.appId))->getAmEventType(),
								START_REDUCE_CONTAINER, APPLICATION, SEIZETOMASTER, waitingMapper.taskLocation);

						eventsList.push(newEvent);
					}
					else{	// mapper
						// attribute carries mapperLocation
						Event newEvent(waitingMapper.appId, (ev->getEventTime()+OVERHEAD_DELAY),HEARTBEAT_SIZE, 0.0, waitingMapper.outEvent, (applications.at(waitingMapper.appId))->getAmEventType(),
								START_MAP_CONTAINER, APPLICATION, SEIZETOMASTER, waitingMapper.taskLocation, waitingMapper.attr, waitingMapper.fsid);

						eventsList.push(newEvent);
					}
				}

				if((applications.at(ev->getApplicationId()))->shuffleReady()){

					// when mappers are ready to shuffle (based on slow start), generate reducers...
					if(!(applications.at(ev->getApplicationId()))->isReducersRequested()){
						// send Reducer Requests
						#ifndef TERMINAL_LOG_DISABLED
						std::cout<< "Generate Reducer Requests for Application: " << ev->getApplicationId() << std::endl;
						#endif
						(applications.at(ev->getApplicationId()))->setReducersRequested(true);

						for(int j=0;j<(applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreduceJobReduces();j++){

							Event newEvent(ev->getApplicationId(), ev->getEventTime()+((j)*0.0001),HEARTBEAT_SIZE,
									0.0, outputEventType_, (applications.at(ev->getApplicationId()))->getRmEventType(), ALLOCATE_RM_RESOURCES_RM, MAPTASK);

							eventsList.push(newEvent);
						}
					}

					// set map finish time
					if((applications.at(ev->getApplicationId()))->getWbCompleted() == (applications.at(ev->getApplicationId()))->getTotalNumberOfMappers()){
						(applications.at(ev->getApplicationId()))->setMapFinishTime(ev->getEventTime());
					}

					// get total number of reducers (ASSUMPTION: the data will be evenly distributed among reducers...)
					int totalReducers = (applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreduceJobReduces();
					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "totalReducers " << totalReducers  << std::endl;
					#endif
					size_t numreadyMappers = (applications.at(ev->getApplicationId()))->getNumberOfReadyMappers();

					for(size_t j=0;j<numreadyMappers;j++){

						spillInfo toBeShuffled = (applications.at(ev->getApplicationId()))->popReadyToShuffleInfo();

						for(int i=0;i< totalReducers;i++){
							// create events to start shuffling for the mappers at their corresponding nodes with the appID (if not already shuffled)
							// store reducer order (to determine which reducer to send data to) in event attribute
							Event newEvent(ev->getApplicationId(), ev->getEventTime(), toBeShuffled.size_, 0, toBeShuffled.nodeEventType_, toBeShuffled.nodeEventType_,
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

		else if(ev->getEventBehavior() == START_SHUFFLE){	/*DOC: start shuffling */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++SHUFFLING: READ+++++++TIME:  " << ev->getEventTime() << " NodeId: " << ev->getDestinationEventType() << " fsID " << ev->getFsId() << " seized: "<< ev->getSeizedResQuantity() << std::endl;
			#endif

			// add to a queue of "reducer creation completion waiters"
			(applications.at(ev->getApplicationId()))->saveReducerCompletionWaiters(ev->getNeededResQuantity(), ev->getDestinationEventType(),
					ev->getEntityIns().getAttribute(), ev->getFsId(), outputEventType_);

			// make sure at least one reducer is created
			if((applications.at(ev->getApplicationId()))->hasAnyReducerCreated()){

				// Overall shuffle start time
				int reducerLocationIndex = ev->getEntityIns().getAttribute();
				if((applications.at(ev->getApplicationId()))->getShuffleStartTime(reducerLocationIndex) < 0){
					(applications.at(ev->getApplicationId()))->setShuffleStartTime(ev->getEventTime(), reducerLocationIndex);
				}

				if((applications.at(ev->getApplicationId()))->isReducerCompletionWaitersHasElement()){	// then there are some map output waiting to be shuffled to reducers
					size_t totalwaiters= (applications.at(ev->getApplicationId()))->gettotalReducerCompletionWaiters();

					for(size_t i=0; i < totalwaiters;i++){

						reducerWaiterData waiterData = (applications.at(ev->getApplicationId()))->getReducerCompletionWaiter(i);
						int finalDest = (applications.at(ev->getApplicationId()))->getReducerLocation(waiterData.attribute_);

						if(finalDest != -1){	// reducer location found (which means reducer was created) and data was not shuffled (since it was waiting in waiterData)

							// add shuffle start time (per shuffle)
							(applications.at(ev->getApplicationId()))->addShuffleStartTime(waiterData.fsID_, waiterData.attribute_, ev->getEventTime());
							// read data at corresponding node to be sent to a reducer (waiterData.attribute_ will be used to get reducer location)
							Event newEvent(ev->getApplicationId(), ev->getEventTime(), waiterData.neededResQuantity_, 0, waiterData.myLoc_, waiterData.myLoc_,
									SHUFFLE_READ_DATA, REDUCETASK, SEIZETOMASTER, waiterData.attribute_, -1, waiterData.fsID_, waiterData.attribute_);
							eventsList.push(newEvent);

							// data shuffle started so set shuffle flag to true
							(applications.at(ev->getApplicationId()))->signalReducerCompletionWaiter(i);
						}
					}
					// clear shuffle started elements from reducer completion waiter list (those who wait fro reducer to be created to start shuffle...)
					(applications.at(ev->getApplicationId()))->clearFinishedReducerCompletionWaiters();
				}
			}
		}

		else if(ev->getEventBehavior() == SHUFFLE_READ_DATA){	/*DOC: SHUFFLE  - Read at remote for waiters*/
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++SHUFFLE READ FOR WAITERS at :" << ev->getRedId() << " TIME: "<< ev->getEventTime() << " BYTES TO BE PROCESSED: "<< ev->getNeededResQuantity() << std::endl;
			#endif

			// read each packet seperately from local disk
			// (packets are assumed to be of size ((applications.at(ev->getApplicationId()))->getRecordSize()))

			double totalSplitSize = ev->getNeededResQuantity();

			// this is experimental
			(applications.at(ev->getApplicationId()))->setShuffleReadStartTime(ev->getFsId(), ev->getEventTime());
			// this will be checked to confirm that a reduce task receives all data in this fsId that was intended for it.
			(applications.at(ev->getApplicationId()))->setShuffledTotalDataForFsidRedid(ev->getFsId(), ev->getRedId(), totalSplitSize);

			int numberOfPackets = totalSplitSize/((applications.at(ev->getApplicationId()))->getRecordSize());
			bool lastRecordExist = false;
			double lastRecordSize;

			if((size_t)numberOfPackets*((applications.at(ev->getApplicationId()))->getRecordSize()) < totalSplitSize){
				lastRecordSize = totalSplitSize - numberOfPackets*((applications.at(ev->getApplicationId()))->getRecordSize());
				lastRecordExist=true;
			}
			if(lastRecordExist){
				(applications.at(ev->getApplicationId()))->setShufflePacketCount(ev->getFsId(), ev->getRedId(), numberOfPackets+1);
			}
			else{
				(applications.at(ev->getApplicationId()))->setShufflePacketCount(ev->getFsId(), ev->getRedId(), numberOfPackets);
			}

			if(numberOfPackets > BUFFER_NUMBER_OF_PACKETS){
				ev->setRecordId(1);

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					hd_.work(HD_READ_DATA_TO_SHUFFLE, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
			else{
				ev->setRecordId(-1);

				for(int i=0;i<numberOfPackets;i++){
					hd_.work(HD_READ_DATA_TO_SHUFFLE, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}

				if(lastRecordExist){
					hd_.work(HD_READ_DATA_TO_SHUFFLE, lastRecordSize, ev, outputEventType_);
				}
			}
			// save remaining number of packets to send and the information whether a last packet exists and the lastpacketsize
			(applications.at(ev->getApplicationId()))->setShuffleReadInfo(ev->getFsId(), ev->getRedId(), numberOfPackets, lastRecordExist, lastRecordSize);
		}

		else if(ev->getEventBehavior() == SHUFFLE_READ_DATA_COMPLETE){ 	/*DOC: start shuffling */
			#ifndef TERMINAL_LOG_DISABLED
						std::cout << "+++++++SHUFFLING: TRANSFER+++++++TIME:  " << ev->getEventTime() << " NodeId: " << ev->getDestinationEventType() << " fsID " << ev->getFsId() <<
								" seized: "<< ev->getSeizedResQuantity() << " needed: " << ev->getNeededResQuantity() <<  std::endl;
			#endif

			int reducerLocationIndex = ev->getEntityIns().getAttribute();
			int finalDest = (applications.at(ev->getApplicationId()))->getReducerLocation(reducerLocationIndex);
			// transfer data to reducer(s) & release read resources for shuffle
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			double shuffleOutputPartSize = ev->getNeededResQuantity();

			if(ev->getDestinationEventType() == finalDest){	// local
				// now each part can be sent to a reducer
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), shuffleOutputPartSize, 0, finalDest, finalDest,
						SHUFFLE_IN_MEM_COLLECTION, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), -1, ev->getFsId(), reducerLocationIndex);

				eventsList.push(newEvent);
			}
			else{
				// now each part can be sent to a reducer
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), shuffleOutputPartSize, 0, outputEventType_, finalDest,
						SHUFFLE_IN_MEM_COLLECTION, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), -1, ev->getFsId(), reducerLocationIndex);

				eventsList.push(newEvent);
			}

			// check if there are more to read for this fsid and redid
			if(ev->getRecordId() == 1 &&
					(applications.at(ev->getApplicationId()))->incShuffleReadBufferCompletedPacketCount(ev->getFsId(), ev->getRedId()) == BUFFER_NUMBER_OF_PACKETS){
				// there are more packets that were not able to fit in buffer
				// update the remaining number of transfer records

				(applications.at(ev->getApplicationId()))->decrementShuffleReadNumberOfRecords(ev->getFsId(), ev->getRedId(),BUFFER_NUMBER_OF_PACKETS);

				// get the remaning number of transfer records
				int remaining = (applications.at(ev->getApplicationId()))->getShuffleReadInfo_remainingNumberOfMapRecords(ev->getFsId(), ev->getRedId());

				if(remaining > BUFFER_NUMBER_OF_PACKETS){	// not the last set

					ev->setSeizedResQuantity(0);
					for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
						// read filesplit from remote and send a response back to the origin

						// send each record seperately.
						hd_.work(HD_READ_DATA_TO_SHUFFLE, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
						ev->incGlobalEntityId();
					}
				}
				else{	// it is the last set
					// last set of records!
					ev->setRecordId(-1);
					ev->setSeizedResQuantity(0);

					for(int i=0;i<remaining;i++){
						// read filesplit from remote and send a response back to the origin
						// send each record seperately.

						hd_.work(HD_READ_DATA_TO_SHUFFLE, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
						ev->incGlobalEntityId();
					}
					if((applications.at(ev->getApplicationId()))->getShuffleReadInfo_lastRecordExist(ev->getFsId(), ev->getRedId())){
						hd_.work(HD_READ_DATA_TO_SHUFFLE, (applications.at(ev->getApplicationId()))->getShuffleReadInfo_lastRecordSize(ev->getFsId(), ev->getRedId()), ev, outputEventType_);
					}
				}
				// reset bufferCompletedPacketCount
				(applications.at(ev->getApplicationId()))->resetShuffleReadBufferCompletedPacketCount(ev->getFsId(), ev->getRedId());
			}

			if(ev->getRecordId() == -1){
				// this is experimental
				(applications.at(ev->getApplicationId()))->setShuffleReadFinTime(ev->getFsId(), ev->getEventTime());
			}
		}
		else if(ev->getEventBehavior() == SHUFFLE_WRITE_DATA){	/*DOC: start shuffling write (now the transferred data is written to REDUCER node) */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++SHUFFLING: WRITE+++++++TIME:   " << ev->getEventTime() << " NodeId: " <<
					ev->getDestinationEventType() << " needed " << ev->getNeededResQuantity()<< " fsID " << ev->getFsId() << " seized: "<< ev->getSeizedResQuantity() << std::endl;
			#endif

			double totalSplitSize = ev->getNeededResQuantity();
			int numberOfPackets = totalSplitSize/((applications.at(ev->getApplicationId()))->getRecordSize());

			bool lastRecordExist = false;
			double lastRecordSize;

			if((size_t)numberOfPackets*((applications.at(ev->getApplicationId()))->getRecordSize()) < totalSplitSize){
				lastRecordSize = totalSplitSize - numberOfPackets*((applications.at(ev->getApplicationId()))->getRecordSize());
				lastRecordExist=true;
			}
			size_t myId = getUniqueId();
			ev->setSpillTally(myId);

			if(lastRecordExist){
				// this will be checked to confirm that a reduce task receives all data in this fsId that was intended for it.
				(applications.at(ev->getApplicationId()))->setShuffleWriteDataProperties(myId, numberOfPackets+1, totalSplitSize, lastRecordExist, lastRecordSize);
			}
			else{
				// this will be checked to confirm that a reduce task receives all data in this fsId that was intended for it.
				(applications.at(ev->getApplicationId()))->setShuffleWriteDataProperties(myId, numberOfPackets, totalSplitSize, lastRecordExist, lastRecordSize);
			}
			if(numberOfPackets > BUFFER_NUMBER_OF_PACKETS){
				ev->setRecordId(1);

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
			else{
				ev->setRecordId(-1);

				for(int i=0;i<numberOfPackets;i++){
					hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
					ev->incGlobalEntityId();
				}
				if(lastRecordExist){
					hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, lastRecordSize, ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
		}
		else if(ev->getEventBehavior() == SHUFFLE_WRITE_DATA_PARTIALLY_COMPLETE){

			// this will generate SHUFFLE_WRITE_DATA_COMPLETE event when all the parts of data is received

			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			if(ev->getRecordId() == 1 &&
					(applications.at(ev->getApplicationId()))->incShuffleWriteDataCount(ev->getSpillTally()) == BUFFER_NUMBER_OF_PACKETS){

				// there are more packets that were not able to fit in buffer
				// update the remaining number of transfer records
				(applications.at(ev->getApplicationId()))->decrementShuffleWriteDataRecords(ev->getSpillTally(),BUFFER_NUMBER_OF_PACKETS);

				// get the remaning number of transfer records
				int remaining = (applications.at(ev->getApplicationId()))->getShuffleWriteData_numPackets(ev->getSpillTally());

				ev->setSeizedResQuantity(0);
				if(remaining > BUFFER_NUMBER_OF_PACKETS){	// not the last set

					for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
						// read filesplit from remote and send a response back to the origin

						// send each record seperately.
						hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
						ev->incGlobalEntityId();
					}
				}
				else{	// it is the last set
					// last set of records!
					ev->setRecordId(-1);

					for(int i=0;i<remaining;i++){
						// read filesplit from remote and send a response back to the origin
						// send each record seperately.

						hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, ((applications.at(ev->getApplicationId()))->getRecordSize()), ev, outputEventType_);
						ev->incGlobalEntityId();
					}
					if((applications.at(ev->getApplicationId()))->getShuffleWriteData_lastRecordExist(ev->getSpillTally())){

						hd_.work(HD_WRITE_SHUFFLE_DATA_TO_DISK, (applications.at(ev->getApplicationId()))->getShuffleWriteData_lastRecordSize(ev->getSpillTally()), ev, outputEventType_);
					}
				}
				// reset bufferCompletedPacketCount
				(applications.at(ev->getApplicationId()))->resethuffleWriteDataCount(ev->getSpillTally());
			}

			if(ev->getRecordId() == -1){

				if(!(applications.at(ev->getApplicationId()))->decrementShuffleWriteDataRecords(ev->getSpillTally(),1)){	// done

					// start processing the data (merge)
					// create an event to call event START_REDUCE_SORT_FUNC
					Event newEvent(ev->getApplicationId(), ev->getEventTime(), (applications.at(ev->getApplicationId()))->getShuffleWriteDataProperties_DataSize(ev->getSpillTally()), 0, ev->getDestinationEventType(), ev->getDestinationEventType(),
							SHUFFLE_WRITE_DATA_COMPLETE, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());

					eventsList.push(newEvent);
				}
			}
		}
		else if(ev->getEventBehavior() == ON_DISK_MERGE_READ_COMPLETE){	/*DOC: Read Completed for On Disk Merge data from hard disk */
			// release On Disk Merge Data resources from HD
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			//cpu_.reduceMergeWork(memory_.getResetDataWaitingForSpill(ev->getRedId()), (applications.at(ev->getApplicationId()))->getSeizedReduceCpuVcores(), ev, (applications.at(ev->getApplicationId()))->getReduceIntensity());
			cpu_.reduceMergeWork(ev->getNeededResQuantity(), (applications.at(ev->getApplicationId()))->getSeizedReduceCpuVcores(), ev, 0.1);
		}
		else if(ev->getEventBehavior() == SHUFFLE_WRITE_DATA_COMPLETE){	/*DOC: Shuffle Releases write resources. */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++SHUFFLING: WRITE COMPLETED at " << ev->getRedId() <<  " TIME:   " << ev->getEventTime() << " NodeId: " << ev->getDestinationEventType() << " fsID " << ev->getFsId() <<
					" seized: "<< ev->getSeizedResQuantity() << std::endl;
			#endif

			// add data to the waiting list for merge
			(applications.at(ev->getApplicationId()))->adddataWaitingToBeMerged_(ev->getDestinationEventType(), ev->getNeededResQuantity(), ev->getRedId());
			(applications.at(ev->getApplicationId()))->notifyReduceWriteComplete(ev->getDestinationEventType(), ev->getRedId());

			// get each shuffle time
			int mergeID = ev->getEntityIns().getAttribute();
			if(mergeID != -1){	// request not coming from ON-DISK MERGE

			}
			else{
				#ifndef TERMINAL_LOG_DISABLED
				std::cout<< "ON DISK MERGE COMPLETED " << ev->getEventTime() << " redId " << ev->getRedId() << std::endl;
				#endif
				int remainingReadyMergeCount = (applications.at(ev->getApplicationId()))->subReadyForOnDiskMergeCount_(ev->getRedId());

				if(remainingReadyMergeCount > 0){
					#ifndef TERMINAL_LOG_DISABLED
					std::cout<< "ON DISK MERGE STARTED " << ev->getEventTime() << " redId " << ev->getRedId() << std::endl;
					#endif
					size_t totalSizeOfNewMergedFile = (applications.at(ev->getApplicationId()))->popMergeSize(ev->getDestinationEventType(), ev->getRedId());

					ev->setNeededResQuantity(totalSizeOfNewMergedFile);
					// set attribute to -1 to let shuffle time saver know that request is coming from on disk merge...
					ev->setEntityInsAttribute(-1);

					// read On Disk Merge Data
					hd_.work(ON_DISK_MERGE_READ_COMPLETE, totalSizeOfNewMergedFile, ev, outputEventType_);
				}
			}

			// if the number of files on disk is greater than  2 * mapreduce.task.io.sort.factor - 1, then start "on-disk merge" of mapreduce.task.io.sort.factor files
			size_t numberofWaiting2BeMerged = (applications.at(ev->getApplicationId()))->getNumberOfWaitingTobeMerged(ev->getDestinationEventType(), ev->getRedId());
			int io_sortFactor = (applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreducetaskIoSortFactor();

			if((applications.at(ev->getApplicationId()))->datareceivedFromAllMappers(ev->getDestinationEventType(), ev->getRedId()) &&
				(applications.at(ev->getApplicationId()))->getReduceSpillCount(ev->getDestinationEventType(), ev->getRedId()) ==
				(applications.at(ev->getApplicationId()))->getReduceWriteCount(ev->getDestinationEventType(), ev->getRedId())
				){ // perform the reduce SORT and pass the output to reducer
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "START REDUCE SORT " << ev->getEventTime() << " Reducer: "   <<  ev->getRedId() << " Location "  << ev->getDestinationEventType() << std::endl;
				#endif

				(applications.at(ev->getApplicationId()))->setShuffleFinishTime(ev->getEventTime(), ev->getRedId());

				// reduce start (per reducer)
				// the last non-On-disk write will set this value for each reducer
				(applications.at(ev->getApplicationId()))->addReduceStartTime(ev->getRedId(), ev->getEventTime());

				// send numberofWaiting2BeMerged within attribute of an event
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), -1, -1, ev->getDestinationEventType(), ev->getDestinationEventType(),
						START_REDUCE_SORT_FUNC, REDUCETASK, SEIZETOMASTER, numberofWaiting2BeMerged, ev->getFsLoc(), ev->getFsId(), ev->getRedId());

				eventsList.push(newEvent);

				//reset numberofWaiting2BeMerged
				numberofWaiting2BeMerged = 0;
			}

			if(numberofWaiting2BeMerged >= (2 * (size_t)io_sortFactor)-1){
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << " ON DISK MERGE!"  << ev->getRedId() << " numberofWaiting2BeMerged " << numberofWaiting2BeMerged<< std::endl;
				#endif
				// merge the last "mapreduce.task.io.sort.factor" files

				bool isOngoingOnDiskMerge = (applications.at(ev->getApplicationId()))->addReadyForOnDiskMergeCount_(ev->getRedId());

				// reduce ready to be merged count
				(applications.at(ev->getApplicationId()))->reduceNumberOfChunksWaitingToBeMerged(ev->getDestinationEventType(), ev->getRedId());


				// notify future on disk merge write
				(applications.at(ev->getApplicationId()))->notifyIncomingOnDiskMergeWrite(ev->getDestinationEventType(), ev->getRedId());

				if(!isOngoingOnDiskMerge){

					size_t totalSizeOfNewMergedFile = (applications.at(ev->getApplicationId()))->popMergeSize(ev->getDestinationEventType(), ev->getRedId());

					ev->setNeededResQuantity(totalSizeOfNewMergedFile);
					// set attribute to -1 to let shuffle time saver know that request is coming from on disk merge...
					ev->setEntityInsAttribute(-1);

					// read On Disk Merge Data
					hd_.work(ON_DISK_MERGE_READ_COMPLETE, totalSizeOfNewMergedFile, ev, outputEventType_);
				}
			}
			// otherwise wait for completion of shuffle. when shuffle is completed, "sort" phase of reduce will begin (actually "final merge" is a better name for this phase )
		}

		else if(ev->getEventBehavior() == START_REDUCE_SORT_FUNC){	// reduce sort (will repetitively be called until remaining files per reducer are less than mapreduce.task.io.sort.factor)

			int io_sortFactor = (applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreducetaskIoSortFactor();
			int numberofWaiting2BeMerged = ev->getEntityIns().getAttribute();

			if(numberofWaiting2BeMerged <= io_sortFactor){	// now feed the output of sort to reduce function
				// reduce start (overall)
				if((applications.at(ev->getApplicationId()))->getReduceStartTime() < 0){
					(applications.at(ev->getApplicationId()))->setReduceStartTime(ev->getEventTime());
				}
				// here read the shuffled data for reduceFunctionWork

				// calculate reduce internal output partition size that needs to be read from local hd by this reducer
				size_t reduceOutputPartSize = ((applications.at(ev->getApplicationId()))->getApplicationSize() * ((applications.at(ev->getApplicationId()))->getReduceOutputVolume())/100.0) / (((applications.at(ev->getApplicationId()))->getMapReduceConfig()).getMapreduceJobReduces());

				int numberOfReduceRecords = reduceOutputPartSize/((applications.at(ev->getApplicationId()))->getRecordSize());

				size_t eachRecordSize=0;
				bool lastRecordExist = false;
				size_t lastRecordSize;

				if((size_t)numberOfReduceRecords*((applications.at(ev->getApplicationId()))->getRecordSize()) < reduceOutputPartSize){
					lastRecordSize = reduceOutputPartSize - (size_t)numberOfReduceRecords*((applications.at(ev->getApplicationId()))->getRecordSize());
					lastRecordExist=true;
				}
				if(lastRecordExist && numberOfReduceRecords){
					eachRecordSize = ((applications.at(ev->getApplicationId()))->getRecordSize());
					(applications.at(ev->getApplicationId()))->setReduceRecordCount(ev->getRedId(), numberOfReduceRecords+1);
				}
				else{

					if(numberOfReduceRecords){
						eachRecordSize = reduceOutputPartSize/(numberOfReduceRecords);
					}
					if(lastRecordExist){
						(applications.at(ev->getApplicationId()))->setReduceRecordCount(ev->getRedId(), numberOfReduceRecords+1);
					}
					else{
						(applications.at(ev->getApplicationId()))->setReduceRecordCount(ev->getRedId(), numberOfReduceRecords);
					}
				}
				(applications.at(ev->getApplicationId()))->setRecordInfo(ev->getRedId(), eachRecordSize, lastRecordSize, lastRecordExist, numberOfReduceRecords, false);
				// create a new event for the first record to be read at HD
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), -1, -1, ev->getDestinationEventType(), ev->getDestinationEventType(),
						READ_REDUCE_RECORDS_TO_SORT, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), -1, BUFFER_NUMBER_OF_PACKETS);

				eventsList.push(newEvent);
			}
			else if(numberofWaiting2BeMerged >= 2*io_sortFactor-1){		// sort& merge 10 files

				// upon return this will be numberofWaiting2BeMerged (put this into the attribute of the event)
				numberofWaiting2BeMerged -= (io_sortFactor-1);

				size_t totalSizeOfNewMergedFile = (applications.at(ev->getApplicationId()))->popMergeSize(ev->getDestinationEventType(), ev->getRedId());

				ev->setNeededResQuantity(totalSizeOfNewMergedFile);
				ev->setEntityInsAttribute(numberofWaiting2BeMerged);

				// read ToBeSorted Reduce Data
				hd_.work(HD_READ_TOBE_SORTED_REDUCE_DATA, totalSizeOfNewMergedFile, ev, outputEventType_);
			}
			else{	// numberofWaiting2BeMerged < 2*io_sortFactor-1 && numberofWaiting2BeMerged > io_sortFactor

				int toBeMergedCount = 1+ numberofWaiting2BeMerged-io_sortFactor;

				// upon return this will be numberofWaiting2BeMerged (put this into the attribute of the event)
				numberofWaiting2BeMerged -= (numberofWaiting2BeMerged-io_sortFactor);

				size_t totalSizeOfNewMergedFile = (applications.at(ev->getApplicationId()))->popMergeGivenSize(ev->getDestinationEventType(), ev->getRedId(), toBeMergedCount);

				ev->setNeededResQuantity(totalSizeOfNewMergedFile);
				ev->setEntityInsAttribute(numberofWaiting2BeMerged);

				// read ToBeSorted Reduce Data
				hd_.work(HD_READ_TOBE_SORTED_REDUCE_DATA, totalSizeOfNewMergedFile, ev, outputEventType_);
			}
		}
		else if(ev->getEventBehavior() == RELEASE_AND_START_REDUCE_SORT){	// release and start reduce sort

			// release ToBeSorted Reduce Data resources from HD
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			cpu_.reduceSort(ev->getNeededResQuantity(), (applications.at(ev->getApplicationId()))->getSeizedReduceCpuVcores(), ev, (applications.at(ev->getApplicationId()))->getReduceSortIntensity());
		}
		else if(ev->getEventBehavior() == RELEASE_AND_FINISH_REDUCE_SORT){	// release and finish reduce sort
			cpu_.decreduceCpuUserCount(ev->getRedId());
			// write output to disk and upon completion of disk write create new reduce sort event (event START_REDUCE_SORT_FUNC)
			// write sorted Reduce Data to disk
			hd_.work(HD_WRITE_SORTED_REDUCE_DATA, ev->getNeededResQuantity(), ev, outputEventType_);
		}
		else if(ev->getEventBehavior() == RELEASE_AND_DONE_REDUCE_SORT){	// release and finish reduce sort

			// release write resources for sorted Reduce Data
			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			// push new file on waiting to be merged
			(applications.at(ev->getApplicationId()))->adddataWaitingToBeMerged_(ev->getDestinationEventType(), ev->getNeededResQuantity(), ev->getRedId());

			// create an event to call event START_REDUCE_SORT_FUNC
			Event newEvent(ev->getApplicationId(), ev->getEventTime(), ev->getNeededResQuantity(), 0, ev->getDestinationEventType(), ev->getDestinationEventType(),
					START_REDUCE_SORT_FUNC, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());

			eventsList.push(newEvent);
		}
		else if(ev->getEventBehavior() == READ_REDUCE_RECORDS_TO_SORT){
			recordInfo newRecord = (applications.at(ev->getApplicationId()))->getReduceRecordInfo(ev->getRedId());

			if(newRecord.remainingRecordCount_ > BUFFER_NUMBER_OF_PACKETS){	// send BUFFER_NUMBER_OF_PACKETS now. upon completion send more...
				// decrement remaining reduce record count
				(applications.at(ev->getApplicationId()))->decrementRecordInfoRemainingReduceRecordCountAmount(ev->getRedId(), BUFFER_NUMBER_OF_PACKETS);
				ev->setRecordId(newRecord.remainingRecordCount_);

				for(int i=0;i<BUFFER_NUMBER_OF_PACKETS;i++){
					// read each record and upon completion release the seized resource and start map function on the record just read
					hd_.work(HD_READ_SHUFFLED_DATA, newRecord.eachRecordSize_, ev, outputEventType_);
					ev->incGlobalEntityId();
				}
			}
			else{
				// reset remaining reduce record count
				(applications.at(ev->getApplicationId()))->decrementRecordInfoRemainingReduceRecordCountAmount(ev->getRedId(), newRecord.remainingRecordCount_);

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
		else if(ev->getEventBehavior() == SHUFFLE_IN_MEM_COLLECTION){	/*DOC: Shuffle In-memory collection. */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++SHUFFLING: IN-MEMORY COLLECTION:   " << ev->getEventTime() <<  " NodeId: " << ev->getDestinationEventType() << " fsID " << ev->getFsId() <<
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
			bool isExceedingMaxSingleShuffleLimit = (applications.at(ev->getApplicationId()))->incShuffleCollectedDataAmount(ev->getFsId(), ev->getRedId(), ev->getNeededResQuantity());

			if(isExceedingMaxSingleShuffleLimit){
				(applications.at(ev->getApplicationId()))->incShuffleFlushedDataAmount(ev->getFsId(), ev->getRedId());
			}
			// CASE2: startMergeSpill
			bool startMergeSpill = memory_.addCheckMergeSpill(ev->getNeededResQuantity(), (applications.at(ev->getApplicationId()))->getShuffleMergeLimit(), ev->getRedId());


			// for each received packet, notify data reception (this is used to determine whether all packets from all map tasks are received)
			(applications.at(ev->getApplicationId()))->notifyDataReception(ev->getDestinationEventType(), ev->getRedId(), ev->getFsId());

			if(isExceedingMaxSingleShuffleLimit || startMergeSpill || (applications.at(ev->getApplicationId()))->datareceivedFromAllMappers(ev->getDestinationEventType(), ev->getRedId())){

					// total number of spills on disk for this reduce task
					(applications.at(ev->getApplicationId()))->notifyReduceSpillReception(ev->getDestinationEventType(), ev->getRedId());


					// more than 1 map outputs in memory (need to merge them)
					if(memory_.getnumberOfMapOutputsWaitingForSpill(ev->getRedId()) > 1){
						//cpu_.reduceMergeWork(memory_.getResetDataWaitingForSpill(ev->getRedId()), (applications.at(ev->getApplicationId()))->getSeizedReduceCpuVcores(), ev, (applications.at(ev->getApplicationId()))->getReduceIntensity());
						cpu_.reduceMergeWork(memory_.getResetDataWaitingForSpill(ev->getRedId()), (applications.at(ev->getApplicationId()))->getSeizedReduceCpuVcores(), ev, 0.1);
					}
					else{
						Event newEvent(ev->getApplicationId(), ev->getEventTime(), memory_.getResetDataWaitingForSpill(ev->getRedId()), 0, ev->getDestinationEventType(), ev->getDestinationEventType(),
								SHUFFLE_WRITE_DATA, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());

						eventsList.push(newEvent);
					}
			}
		}
		else if(ev->getEventBehavior() == REDUCER_IN_MEM_INTER_MERGE){	/*DOC: REDUCER In-memory intermediate merge */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++SHUFFLING: IN-MEMORY PROCESSING...: TIME: "<< ev->getEventTime() << "NEEDED "<< ev->getNeededResQuantity() << " seized: "<< ev->getSeizedResQuantity() << std::endl;
			#endif
			cpu_.decreduceCpuUserCount(ev->getRedId());

			Event newEvent(ev->getApplicationId(), ev->getEventTime(), ev->getNeededResQuantity(), 0, ev->getDestinationEventType(), ev->getDestinationEventType(),
					SHUFFLE_WRITE_DATA, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());

			eventsList.push(newEvent);
		}

		else if(ev->getEventBehavior() == START_REDUCE_FUNC){	/*DOC: Start reduce function */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++REDUCE FUNCTION STARTED at :" << ev->getRedId() << " TIME: "<< ev->getEventTime() << " BYTES TO BE PROCESSED: "<< ev->getNeededResQuantity() << std::endl;
			#endif

			// release "read shuffled output" resources in reducer
			hd_.work(READ_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);

			cpu_.reduceFunctionWork(ev->getNeededResQuantity(),
					(applications.at(ev->getApplicationId()))->getSeizedReduceCpuVcores(), ev, (applications.at(ev->getApplicationId()))->getReduceIntensity());
		}

		else if(ev->getEventBehavior() == FINISH_REDUCE_FUNC){	/*DOC: Finish reduce function */
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "+++++++REDUCE FUNCTION COMPLETED at :" << ev->getRedId() << " TIME: "<< ev->getEventTime() << " BYTES TO BE WRITTEN: "<< ev->getNeededResQuantity() << std::endl;
			#endif

			cpu_.decreduceCpuUserCount(ev->getRedId());
			// calculate final output partition that needs to be written to HDFS by this reducer
			double finalOutputPartSize = ev->getNeededResQuantity()*((applications.at(ev->getApplicationId()))->getFinalOutputVolume()/(applications.at(ev->getApplicationId()))->getReduceOutputVolume());

			hd_.work(HD_WRITE_REDUCE_OUTPUT, finalOutputPartSize, ev, outputEventType_);
		}
		else if(ev->getEventBehavior() == WRITE_REDUCE_OUTPUT){	/*DOC: Reduce output written to output file! */

			hd_.work(WRITE_RESOURCE_RELEASE, ev->getSeizedResQuantity(), ev, outputEventType_);
			// when all the reduce records arrive here, then reduce output completed writing its data for this reduce task..
			if((applications.at(ev->getApplicationId()))->areAllReduceRecordComplete(ev->getRedId())){
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "+++++++REDUCE OUTPUT WRITTEN TO FILE: TIME: "<< ev->getEventTime()  << " BYTES WRITTEN: "<< ev->getNeededResQuantity() << std::endl;
				#endif

				// add reduce finish time
				(applications.at(ev->getApplicationId()))->addReduceFinishTime(ev->getRedId(), ev->getEventTime());

				// this reducer has completed its task!
				(applications.at(ev->getApplicationId()))->notifyCompletedReducers();

				// release reducer resources
				memory_.setRemainingCapacity(memory_.getRemainingCapacity() + (applications.at(ev->getApplicationId()))->getSeizedMapreduceReduceMemory());
				struct waitingTaskInfo waitingReducer = g_scheduler.schedulerReleaseReducerresources(ev->getApplicationId(), ev->getDestinationEventType());

				if(waitingReducer.taskLocation != -2){

					if(waitingReducer.taskType){	// reducer

						(applications.at(ev->getApplicationId()))->addReducerLocations(waitingReducer.taskLocation);

						Event newEvent(waitingReducer.appId, (ev->getEventTime()+OVERHEAD_DELAY),HEARTBEAT_SIZE, 0.0, waitingReducer.outEvent, (applications.at(waitingReducer.appId))->getAmEventType(),
								START_REDUCE_CONTAINER, APPLICATION, SEIZETOMASTER, waitingReducer.taskLocation);
						eventsList.push(newEvent);
					}
					else{ // mapper
						// attribute carries mapperLocation
						Event newEvent(waitingReducer.appId, (ev->getEventTime()+OVERHEAD_DELAY),HEARTBEAT_SIZE, 0.0, waitingReducer.outEvent, (applications.at(waitingReducer.appId))->getAmEventType(),
								START_MAP_CONTAINER, APPLICATION, SEIZETOMASTER, waitingReducer.taskLocation, waitingReducer.attr, waitingReducer.fsid);
						eventsList.push(newEvent);
					}
				}
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "--------------SIMULATION COMPLETED FOR REDUCER ID: " << ev->getRedId() << " OF APPLICATION ID: "<< ev->getApplicationId() << std::endl;
				#endif
				// SIMULATION FINISHED FOR THIS REDUCER!
				if((applications.at(ev->getApplicationId()))->checkIfAllReducersComplete()){	// has all reducers been completed?
					#ifndef TERMINAL_LOG_DISABLED
					std::cout<< "---------------------------------------------------------------|\n---------------------------------------------------------------|\n" <<
							"OVERALL SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS " << ev->getEventTime() << " !" << std::endl;
					#endif

					if(!terminal_output_disabled){
						std::cout<< "---------------------------------------------------------------|\n" <<
													"OVERALL SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS " << ev->getEventTime() << " !" << std::endl;
					}
					(applications.at(ev->getApplicationId()))->setReduceFinishTime(ev->getEventTime());

					double mapTime = (applications.at(ev->getApplicationId()))->getMapFinishTime() - (applications.at(ev->getApplicationId()))->getMapStartTime();
//					double reduceTime = (applications.at(ev->getApplicationId()))->getReduceFinishTime() - (applications.at(ev->getApplicationId()))->getReduceStartTime();
					double shuffleTime = (applications.at(ev->getApplicationId()))->getTotalShuffleAvgAmongReducers();
					double avgRedTime =  (applications.at(ev->getApplicationId()))->avgReduceTime();

					if(avgRedTime < 0.01){
						avgRedTime = 0;
					}

					#ifndef TERMINAL_LOG_DISABLED
					std::cout << "MAP SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS "<< mapTime <<  std::endl;
//					std::cout << "REDUCE SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS "<< reduceTime <<  std::endl;
					std::cout << "AVG REDUCE SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS "<<avgRedTime<<  std::endl;
					std::cout << "SHUFFLE SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS "<< shuffleTime <<  std::endl;
					std::cout << "AVERAGE MAP SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS "<< (applications.at(ev->getApplicationId()))->avgMapTime() <<  std::endl;
					#endif

					if(!terminal_output_disabled){
						//std::cout << "REDUCE SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS "<< reduceTime <<  std::endl;
						std::cout << "AVERAGE MAP SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS "<< (applications.at(ev->getApplicationId()))->avgMapTime() <<  std::endl;
						std::cout << "AVG REDUCE SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS "<<avgRedTime<<  std::endl;
						std::cout << "SHUFFLE SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS "<< shuffleTime <<  std::endl;
						std::cout << "MAP SIMULATION TIME FOR APPLICATION " << ev->getApplicationId() << " IS "<< mapTime <<  std::endl;
					}

					// finally release AM resources
					memory_.setRemainingCapacity(memory_.getRemainingCapacity() + ((applications.at(ev->getApplicationId()))->getAmResourceMb()*ONE_MB_IN_BYTES));
					g_scheduler.schedulerReleaseAMresources(ev->getApplicationId());

					#ifdef TESTING_ENABLED
					testing((applications.at(ev->getApplicationId()))->getMapReduceConfig().getMapreduceJobReduces(), ev->getEventTime(),
							(applications.at(ev->getApplicationId()))->avgMapTime(), avgRedTime, shuffleTime, mapTime);
					#endif
				}
			}
		}
		//-------------------------------------------------------------------------------------DELAY-----------------------------------------------------------------------//
		//-----------------------------------------------------------------------------------------------------------------------------------------------------------------//
		else if(ev->getEventBehavior() == FS_RETRIEVAL){

			// Check for Uber task requirements.
			if(!isUberTask(ev)){

				// for each YARN_NODEMANAGER_HEARTBEAT_INTERVALMS, a heartbeat will be sent to RM and the request for resources for map / reduce tasks will be sent.

				// send Mapper Requests
				int i=0;
				int fileSplitNodeExpectedEventType = (applications.at(ev->getApplicationId()))->getFileSplitNodeExpectedEventType(i);
				int fsID=0;

				while(fileSplitNodeExpectedEventType != -1){

					// Allocate resources for mapper
					// fileSplitNodeExpectedEventType attribute will be used to provide rack awareness for mapper locations

					Event newEvent(ev->getApplicationId(), ev->getEventTime()+((i++)*0.0001),HEARTBEAT_SIZE,
							0.0, outputEventType_, (applications.at(ev->getApplicationId()))->getRmEventType(), ALLOCATE_RESOURCES_RM, MAPTASK, SEIZETOMASTER, fileSplitNodeExpectedEventType, ev->getFsLoc(), fsID++);

					fileSplitNodeExpectedEventType = (applications.at(ev->getApplicationId()))->getFileSplitNodeExpectedEventType(i);
					eventsList.push(newEvent);
				}
			}
			else{
				// run tasks in the same JVM as AM
				// TODO: Uber Task Delay! If it is a uber task do not allocate extra resources... Use AM resources.
				printNodeMessages(NODEMANAGER, 5, ev->getApplicationId());

				// read filesplits, complete mappers, write intermediate outputs, read intermediate outputs, do reducers, write final output
			}
		}
		else if(ev->getEventBehavior() == NODE_HD_CFQ_EVENT){
			hd_.work(HD_CFQ_EVENT, ev->getSeizedResQuantity(), ev, outputEventType_);
		}
	}
	else{
		// Application run request submitted
		printNodeMessages(OTHERNODE, ev->getEventBehavior(), ev->getApplicationId());
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
		Event newEvent(ev.getApplicationId(), (ev.getEventTime() + tot_delay), 0.0, 0.0, ev.getDestinationEventType(), ev.getDestinationEventType(), INIT_APPLICATION, APPLICATION);
		eventsList.push(newEvent);
	}

	else if(ev.getEventBehavior() == RETRIEVE_SPLITS){
		// delay for "Retrieve Input Splits" from HDFS.
		Event newEvent(ev.getApplicationId(), (ev.getEventTime() + tot_delay), 0.0, 0.0, ev.getDestinationEventType(), ev.getDestinationEventType(), FS_RETRIEVAL, APPLICATION);
		eventsList.push(newEvent);
	}
}
