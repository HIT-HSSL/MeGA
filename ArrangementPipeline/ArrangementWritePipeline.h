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

extern uint64_t ContainerSize;
uint64_t ArrangementFlushBufferLength = ContainerSize * 1.2;

class ArrangementWritePipeline {
public:
    ArrangementWritePipeline() : taskAmount(0), runningFlag(true), mutexLock(), condition(mutexLock) {
        worker = new std::thread(std::bind(&ArrangementWritePipeline::arrangementWriteCallback, this));
    }

    int addTask(ArrangementWriteTask *arrangementFilterTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(arrangementFilterTask);
        taskAmount++;
        condition.notify();
        return 0;
    }

    ~ArrangementWritePipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:
    void arrangementWriteCallback() {
        pthread_setname_np(pthread_self(), "AWriting Thread");
        ArrangementWriteTask *arrangementWriteTask;
        char pathBuffer[256];
        uint64_t currentVersion = 0;
        uint64_t classIter = 0;
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

            if (arrangementWriteTask->startFlag) {
                currentVersion = arrangementWriteTask->arrangementVersion;
                classIter = 0;

                sprintf(pathBuffer, VersionFilePath.data(), classIter + 1, currentVersion, archiveCID);
                archivedFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);
                archivedBuffer.init();

                sprintf(pathBuffer, ClassFilePath.data(), classIter + 1, currentVersion + 1, activeCID);
                activeFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);
                activeBuffer.init();
            } else if (arrangementWriteTask->classEndFlag) {
                classIter++;

                delete arrangementWriteTask;

                //====================================
                size_t compressedSize = ZSTD_compress(archivedBuffer.compressBuffer, ArrangementFlushBufferLength,
                                                      archivedBuffer.buffer, archivedBuffer.used, ZSTD_CLEVEL_DEFAULT);
                assert(!ZSTD_isError(compressedSize));
                archivedFileOperator->write(archivedBuffer.compressBuffer, compressedSize);
                archivedFileOperator->fsync();
                delete archivedFileOperator;
                archivedFileOperator = nullptr;

                compressedSize = ZSTD_compress(activeBuffer.compressBuffer, ArrangementFlushBufferLength,
                                               activeBuffer.buffer, activeBuffer.used, ZSTD_CLEVEL_DEFAULT);
                assert(!ZSTD_isError(compressedSize));
                activeFileOperator->write(activeBuffer.compressBuffer, compressedSize);
                activeFileOperator->fsync();
                delete activeFileOperator;
                activeFileOperator = nullptr;
                //====================================

                activeCID = 0;
                archiveCID = 0;

                if (classIter < currentVersion) {
                    sprintf(pathBuffer, VersionFilePath.data(), classIter + 1, currentVersion, archiveCID);
                    archivedFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);
                    archivedBuffer.clear();

                    sprintf(pathBuffer, ClassFilePath.data(), classIter + 1, currentVersion + 1, activeCID);
                    activeFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);
                    activeBuffer.clear();
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
                archivedBuffer.write(arrangementWriteTask->writeBuffer, arrangementWriteTask->length);
                if (archivedBuffer.used >= ContainerSize) {
                    size_t compressedSize = ZSTD_compress(archivedBuffer.compressBuffer, ArrangementFlushBufferLength,
                                                          archivedBuffer.buffer, archivedBuffer.used,
                                                          ZSTD_CLEVEL_DEFAULT);
                    assert(!ZSTD_isError(compressedSize));
                    archivedFileOperator->write(archivedBuffer.compressBuffer, compressedSize);
                    archivedFileOperator->fsync();
                    delete archivedFileOperator;
                    archivedFileOperator = nullptr;

                    archiveCID++;
                    sprintf(pathBuffer, VersionFilePath.data(), classIter + 1, currentVersion, archiveCID);
                    archivedFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);
                    archivedBuffer.clear();
                }
                archivedChunks++;
            } else {
                BlockHeader *bhPtr = (BlockHeader *) arrangementWriteTask->writeBuffer;
                activeBuffer.write(arrangementWriteTask->writeBuffer, arrangementWriteTask->length);
                //activeFileOperator->write(arrangementWriteTask->writeBuffer, arrangementWriteTask->length);
                if (!bhPtr->type) {
                    GlobalMetadataManagerPtr->addSimilarFeature(
                            bhPtr->sFeatures,
                            {bhPtr->fp, (uint32_t) classIter + 1, activeCID,
                             arrangementWriteTask->length - sizeof(BlockHeader)});
                }
                if (activeBuffer.used >= ContainerSize) {
                    size_t compressedSize = ZSTD_compress(activeBuffer.compressBuffer, ArrangementFlushBufferLength,
                                                          activeBuffer.buffer, activeBuffer.used, ZSTD_CLEVEL_DEFAULT);
                    assert(!ZSTD_isError(compressedSize));
                    activeFileOperator->write(activeBuffer.compressBuffer, compressedSize);
                    activeFileOperator->fsync();
                    delete activeFileOperator;
                    activeFileOperator = nullptr;

                    activeCID++;
                    sprintf(pathBuffer, ClassFilePath.data(), classIter + 1, currentVersion + 1, activeCID);
                    activeFileOperator = new FileOperator(pathBuffer, FileOpenType::Write);
                    activeBuffer.clear();
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

    WriteBuffer activeBuffer;
    WriteBuffer archivedBuffer;
};

static ArrangementWritePipeline *GlobalArrangementWritePipelinePtr;

#endif //MEGA_ARRANGEMENTWRITEPIPELINE_H
