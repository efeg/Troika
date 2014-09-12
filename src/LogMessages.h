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

#ifndef LOGMESSAGES_H_
#define LOGMESSAGES_H_

#define SUBMIT_JOB 1
#define COPY_APP_RESOURCES 2
#define GET_APP_ID_RM 1
#define SUBMIT_APP_RM 2
#define ALLOCATE_MAP_RESOURCES_RM 3
#define ALLOCATE_REDUCE_RESOURCES_RM 4
#define START_AM_CONTAINER 1
#define LAUNCH_APP_MASTER 2
#define INIT_APPLICATION 3
#define RETRIEVE_SPLITS 4
#define START_MAP_CONTAINER 6
#define START_REDUCE_CONTAINER 7
#define LAUNCH 8
#define RETRIEVE_JOB_RESOURCES 9
#define RUN_TASK 10
#define READ_NEW_RECORD 105
#define FS_RECORD_READ_FINISH 11
#define RELEASE_REMOTE_FS_READ_RES 12
#define RECEIVE_FS_TRANSFER_REQUEST 13
#define WRITE_TRANSFERRED_FS_TO_LOCAL 14
#define RELEASE_FS_TRANSFERRED_WRITE_RES 15
#define RUN_MAP_FUNCTION 16
#define RUN_MAP_SORT 17
#define RUN_COMBINER 175
#define RUN_MAP_SPILL 18
#define MAP_REMAINING_WORK 19
#define MAP_MERGE_READY 20
#define MERGE_PARTITIONS_FOR_REDUCER 205
#define CPU_MAP_MERGE 21
#define MAP_MERGE_WB 22
#define MAP_MERGE_WB_COMPLETE 23
#define START_SHUFFLE 24
#define SHUFFLE_READ_DATA_COMPLETE 25
#define SHUFFLE_WRITE_DATA 26
#define SHUFFLE_WRITE_DATA_PARTIALLY_COMPLETE 261
#define ON_DISK_MERGE_READ_COMPLETE 265
#define SHUFFLE_WRITE_DATA_COMPLETE 27
#define SHUFFLE_IN_MEM_COLLECTION 28
#define START_REDUCE_SORT_FUNC 200
#define RELEASE_AND_START_REDUCE_SORT 201
#define RELEASE_AND_FINISH_REDUCE_SORT 225
#define RELEASE_AND_DONE_REDUCE_SORT 226
#define READ_REDUCE_RECORDS_TO_SORT 275
#define REDUCER_IN_MEM_INTER_MERGE 29
#define FINISH_REDUCE_FUNC 30
#define WRITE_REDUCE_OUTPUT 31
#define FS_RETRIEVAL 32
#define START_REDUCE_FUNC 33
#define SHUFFLE_READ_DATA 34
#define READ_RESOURCE_RELEASE 1
#define WRITE_RESOURCE_RELEASE 4
#define NODE_HD_CFQ_EVENT 100
#define CPU_SPILL_GENERATE_OP 2
#define CPU_MAP_MERGE_OP 5
#define CPU_REDUCE_IN_MEMORY_MERGE 6
#define CPU_REDUCE_FUNC_OP 7
#define CPU_REDUCE_SORT 8
#define CPU_COMBINER_OP 9
#define CPU_MAP_DELAY_OP 42
#define SUSPENDED 1
#define SPILL_INPROGRESS 2
#define NO_SPILL_INPROGRESS 3
#define HD_READ_FOR_MAP_TASK 0
#define HD_READ_FROM_REMOTE 2
#define HD_WRITE_TO_LOCAL 3
#define HD_WRITE_TRANSFERRED_TO_LOCAL 5
#define HD_READ_MAP_OUTPUT 7
#define HD_WRITE_MERGED_DATA 9
#define HD_READ_DATA_TO_SHUFFLE 11
#define HD_WRITE_SHUFFLE_DATA_TO_DISK 13
#define HD_WRITE_REDUCE_OUTPUT 15
#define HD_READ_SHUFFLED_DATA 17
#define HD_CFQ_EVENT 100
#define HD_READ_TOBE_SORTED_REDUCE_DATA 200
#define HD_WRITE_SORTED_REDUCE_DATA 230
#define HD_READ_TO_MERGE 265
#define COMBINER_SOURCE -2
#define MAP_SORT_SOURCE -1
#define ONE_MB_IN_BYTES 1048576
#define LAUNCH_MAP 1
#define LAUNCH_REDUCE 2

#include <stdlib.h>
#include "Datatypes.h"

void printModuleName(enum ModuleType moduleID);
void printNodeMessages(enum NodeType nodeType, int eventBehavior, size_t applicationId);

#endif /* LOGMESSAGES_H_ */
