/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */


#ifndef MEGA_ARRANGEMENTFILTERPIPELINE_H
#define MEGA_ARRANGEMENTFILTERPIPELINE_H

#include "ArrangementWritePipeline.h"
#include "../MetadataManager/MetadataManager.h"

DEFINE_uint64(ArrangementReadBufferLength,
              8388608, "ArrangementBufferLength");

class ArrangementFilterPipeline{
public:
    ArrangementFilterPipeline(): taskAmount(0), runningFlag(true), mutexLock(), condition(mutexLock){
        worker = new std::thread(std::bind(&ArrangementFilterPipeline::arrangementFilterCallback, this));
    }

    int addTask(ArrangementFilterTask* arrangementFilterTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(arrangementFilterTask);
        taskAmount++;
        condition.notify();
    }

    ~ArrangementFilterPipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:

    void arrangementFilterCallback(){
        pthread_setname_np(pthread_self(), "AFilter Thread");
        ArrangementFilterTask* arrangementFilterTask;
        BlockHeader *blockHeader;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {

                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                arrangementFilterTask = taskList.front();
                taskList.pop_front();
            }

            if(unlikely(arrangementFilterTask->startFlag)){
                ArrangementWriteTask* arrangementWriteTask = new ArrangementWriteTask();
                arrangementWriteTask->startFlag = true;
                arrangementWriteTask->arrangementVersion = arrangementFilterTask->arrangementVersion;
                GlobalArrangementWritePipelinePtr->addTask(arrangementWriteTask);
                delete arrangementFilterTask;
                continue;
            }

            if(unlikely(arrangementFilterTask->classEndFlag)){
                ArrangementWriteTask* arrangementWriteTask = new ArrangementWriteTask(true, arrangementFilterTask->classId);
                GlobalArrangementWritePipelinePtr->addTask(arrangementWriteTask);
                delete arrangementFilterTask;
                continue;
            }

            if(unlikely(arrangementFilterTask->finalEndFlag)){
                ArrangementWriteTask* arrangementWriteTask = new ArrangementWriteTask(true);
                arrangementWriteTask->countdownLatch = arrangementFilterTask->countdownLatch;
                GlobalArrangementWritePipelinePtr->addTask(arrangementWriteTask);
                delete arrangementFilterTask;
                printf("ArrangementFilterPipeline finish\n");
                continue;
            }

            uint64_t readoffset = 0;
            uint8_t *bufferPtr = arrangementFilterTask->readBuffer;

            while (readoffset < arrangementFilterTask->length) {
                blockHeader = (BlockHeader *) (bufferPtr + readoffset);

                int r = GlobalMetadataManagerPtr->arrangementLookup(blockHeader->fp);
                if (!r) {
                    ArrangementWriteTask *arrangementWriteTask = new ArrangementWriteTask(
                            (uint8_t *) blockHeader,
                            blockHeader->length + sizeof(BlockHeader),
                            arrangementFilterTask->classId,
                            arrangementFilterTask->arrangementVersion,
                            true);
                    GlobalArrangementWritePipelinePtr->addTask(arrangementWriteTask);
                }else{
                    ArrangementWriteTask *arrangementWriteTask = new ArrangementWriteTask(
                            (uint8_t *) blockHeader,
                            blockHeader->length + sizeof(BlockHeader),
                            arrangementFilterTask->classId,
                            arrangementFilterTask->arrangementVersion,
                            false);
                    GlobalArrangementWritePipelinePtr->addTask(arrangementWriteTask);
                }
                readoffset += sizeof(BlockHeader) + blockHeader->length;
            }
            assert(readoffset == arrangementFilterTask->length);

            delete arrangementFilterTask;
        }
    }

    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<ArrangementFilterTask*> taskList;
    MutexLock mutexLock;
    Condition condition;
};

static ArrangementFilterPipeline* GlobalArrangementFilterPipelinePtr;

#endif //MEGA_ARRANGEMENTFILTERPIPELINE_H
