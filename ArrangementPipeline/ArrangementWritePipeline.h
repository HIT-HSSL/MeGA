/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_ARRANGEMENTWRITEPIPELINE_H
#define MEGA_ARRANGEMENTWRITEPIPELINE_H

#include <string>
#include "../Utility/StorageTask.h"
#include "../Utility/Lock.h"
#include "../Utility/Likely.h"
#include <thread>
#include <functional>
#include <sys/time.h>
#include "gflags/gflags.h"
#include "../Utility/BufferedFileWriter.h"

DEFINE_uint64(ArrangementFlushBufferLength,
              8388608, "ArrangementFlushBufferLength");

class ArrangementWritePipeline{
public:
    ArrangementWritePipeline(): taskAmount(0), runningFlag(true), mutexLock(), condition(mutexLock){
        worker = new std::thread(std::bind(&ArrangementWritePipeline::arrangementWriteCallback, this));
    }

    int addTask(ArrangementWriteTask* arrangementFilterTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(arrangementFilterTask);
        taskAmount++;
        condition.notify();
    }

    ~ArrangementWritePipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:
    void arrangementWriteCallback(){
        pthread_setname_np(pthread_self(), "AWriting Thread");
        ArrangementWriteTask *arrangementWriteTask;
        char pathBuffer[256];
        uint64_t currentVersion = 0;
        uint64_t classIter = 0;
        uint64_t archivedLength = 0;
        uint64_t activeLength = 0;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                arrangementWriteTask = taskList.front();
                taskList.pop_front();
            }

            if(arrangementWriteTask->startFlag){
                VolumeFileHeader versionFileHeader = {
                        .offsetCount = arrangementWriteTask->arrangementVersion
                };
                currentVersion = arrangementWriteTask->arrangementVersion;
                classIter = 0;
                archivedLength = 0;

                sprintf(pathBuffer, VersionFilePath.data(), classIter + 1, currentVersion, archiveCID);
                archivedFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);

                sprintf(pathBuffer, ClassFilePath.data(), classIter + 1, currentVersion + 1, activeCID);
                activeFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);
            } else if (arrangementWriteTask->classEndFlag) {
                classIter++;
                archivedLength = 0;
                activeLength = 0;

                delete arrangementWriteTask;

                activeFileOperator->fsync();
                delete activeFileOperator;
                archivedFileOperator->fsync();
                delete archivedFileOperator;

                activeCID = 0;
                archiveCID = 0;

                if (classIter < currentVersion) {
                    sprintf(pathBuffer, VersionFilePath.data(), classIter + 1, currentVersion, archiveCID);
                    archivedFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);

                    sprintf(pathBuffer, ClassFilePath.data(), classIter + 1, currentVersion + 1, activeCID);
                    activeFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);
                }
                continue;
            } else if (arrangementWriteTask->finalEndFlag) {
                printf("ActiveChunks:%lu, ArchivedChunks:%lu\n", activeChunks, archivedChunks);
//                activeFileOperator->fsync();
//                delete activeFileOperator;
//                archivedFileOperator->fsync();
//                delete archivedFileOperator;

                activeCID = 0;
                archiveCID = 0;

                currentVersion = -1;

                GlobalMetadataManagerPtr->tableRolling();
                arrangementWriteTask->countdownLatch->countDown();
                delete arrangementWriteTask;
                printf("ArrangementWritePipeline finish\n");
                continue;
            } else if (arrangementWriteTask->isArchived) {
                archivedFileOperator->write(arrangementWriteTask->writeBuffer, arrangementWriteTask->length);
                archivedLength += arrangementWriteTask->length;
                if (archivedLength >= ContainerSize) {
                    archivedLength = 0;
                    archiveCID++;
                    archivedFileOperator->fsync();
                    delete archivedFileOperator;

                    sprintf(pathBuffer, VersionFilePath.data(), classIter + 1, currentVersion, archiveCID);
                    archivedFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);
                }
                archivedChunks++;
            } else {
                BlockHeader *bhPtr = (BlockHeader *) arrangementWriteTask->writeBuffer;
                activeFileOperator->write(arrangementWriteTask->writeBuffer, arrangementWriteTask->length);
                if (!bhPtr->type) {
                    GlobalMetadataManagerPtr->addSimilarFeature(
                            bhPtr->sFeatures,
                            {bhPtr->fp, (uint32_t) classIter + 1, activeCID,
                             arrangementWriteTask->length - sizeof(BlockHeader)});
                }
                activeLength += arrangementWriteTask->length;
                if (activeLength >= ContainerSize) {
                    activeLength = 0;
                    activeCID++;
                    activeFileOperator->fsync();
                    delete activeFileOperator;

                    sprintf(pathBuffer, ClassFilePath.data(), classIter + 1, currentVersion + 1, activeCID);
                    activeFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);
                }
                activeChunks++;
            }
        }
    }

    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<ArrangementWriteTask *> taskList;
    MutexLock mutexLock;
    Condition condition;

    FileOperator *archivedFileOperator = nullptr;

    FileOperator *activeFileOperator = nullptr;

    uint64_t activeCID = 0;
    uint64_t archiveCID = 0;

    uint64_t activeChunks = 0, archivedChunks = 0;
};

static ArrangementWritePipeline* GlobalArrangementWritePipelinePtr;

#endif //MEGA_ARRANGEMENTWRITEPIPELINE_H
