/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_ARRANGEMENTREADPIPELINE_H
#define MEGA_ARRANGEMENTREADPIPELINE_H

#include "ArrangementFilterPipeline.h"
#include "../Utility/FileOperator.h"

extern std::string LogicFilePath;
extern std::string ClassFilePath;
extern std::string VersionFilePath;

class ArrangementReadPipeline{
public:
    ArrangementReadPipeline(): taskAmount(0), runningFlag(true), mutexLock(), condition(mutexLock){
        worker = new std::thread(std::bind(&ArrangementReadPipeline::arrangementReadCallback, this));
    }

    int addTask(ArrangementTask *arrangementTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(arrangementTask);
        taskAmount++;
        condition.notify();
    }

    ~ArrangementReadPipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }


private:

    void arrangementReadCallback() {
        pthread_setname_np(pthread_self(), "AReading Thread");
        ArrangementTask *arrangementTask;
        readAmount = 0;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                arrangementTask = taskList.front();
                taskList.pop_front();
            }

            uint64_t arrangementVersion = arrangementTask->arrangementVersion;

            if (likely(arrangementVersion > 0)) {
                ArrangementFilterTask *startTask = new ArrangementFilterTask();
                startTask->startFlag = true;
                startTask->arrangementVersion = arrangementVersion;
                GlobalArrangementFilterPipelinePtr->addTask(startTask);

                readClassWithAppend(1, arrangementVersion);
                for (uint64_t i = 2; i <= arrangementVersion; i++) {
                    readClass(i, arrangementVersion);
                }

                ArrangementFilterTask *arrangementFilterTask = new ArrangementFilterTask(true);
                arrangementFilterTask->countdownLatch = arrangementTask->countdownLatch;
                GlobalArrangementFilterPipelinePtr->addTask(arrangementFilterTask);
                printf("ArrangementReadPipeline finish, with %lu bytes loaded from %lu categories\n", readAmount,
                       arrangementVersion);
            } else {
                printf("Do not need arrangement, skip\n");
                GlobalMetadataManagerPtr->tableRolling();
                arrangementTask->countdownLatch->countDown();
            }
        }
    }

    uint64_t readClass(uint64_t classId, uint64_t versionId) {
        uint64_t cid = 0;
        while (1) {
            char pathbuffer[512];
            sprintf(pathbuffer, ClassFilePath.data(), classId, versionId, cid);
            FileOperator classFile((char *) pathbuffer, FileOpenType::TRY);
            if (!classFile.ok()) {
                break;
            }
            uint8_t *buffer = (uint8_t *) malloc(FLAGS_ArrangementReadBufferLength);
            uint64_t readSize = classFile.read(buffer, FLAGS_ArrangementReadBufferLength);
            readAmount += readSize;
            ArrangementFilterTask *arrangementFilterTask = new ArrangementFilterTask(buffer, readSize, classId,
                                                                                     versionId);
            GlobalArrangementFilterPipelinePtr->addTask(arrangementFilterTask);
            remove(pathbuffer);
            cid++;
        }
        printf("Read %lu containers from LC(%lu,%lu)\n", cid + 1, classId, versionId);
        ArrangementFilterTask *arrangementFilterTask = new ArrangementFilterTask(true, classId);
        GlobalArrangementFilterPipelinePtr->addTask(arrangementFilterTask);

        return 0;
    }

    uint64_t readClassWithAppend(uint64_t classId, uint64_t versionId) {
        uint64_t cid = 0;
        while (1) {
            char pathbuffer[512];
            sprintf(pathbuffer, ClassFilePath.data(), classId, versionId, cid);
            FileOperator classFile((char *) pathbuffer, FileOpenType::TRY);
            if (!classFile.ok()) {
                break;
            }
            uint8_t *buffer = (uint8_t *) malloc(FLAGS_ArrangementReadBufferLength);
            uint64_t readSize = classFile.read(buffer, FLAGS_ArrangementReadBufferLength);
            readAmount += readSize;
            ArrangementFilterTask *arrangementFilterTask = new ArrangementFilterTask(buffer, readSize, classId,
                                                                                     versionId);
            GlobalArrangementFilterPipelinePtr->addTask(arrangementFilterTask);
            remove(pathbuffer);
            cid++;
        }
        printf("Read %lu containers from LC(%lu,%lu)\n", cid, classId, versionId);

        cid = 0;
        while (1) {
            char pathbuffer[512];
            sprintf(pathbuffer, ClassFileAppendPath.data(), classId, versionId, cid);
            FileOperator classFile((char *) pathbuffer, FileOpenType::TRY);
            if (!classFile.ok()) {
                break;
            }
            uint8_t *buffer = (uint8_t *) malloc(FLAGS_ArrangementReadBufferLength);
            uint64_t readSize = classFile.read(buffer, FLAGS_ArrangementReadBufferLength);
            readAmount += readSize;
            ArrangementFilterTask *arrangementFilterTask = new ArrangementFilterTask(buffer, readSize, classId,
                                                                                     versionId);
            GlobalArrangementFilterPipelinePtr->addTask(arrangementFilterTask);
            remove(pathbuffer);
            cid++;
        }
        printf("Read %lu containers from LC(%lu,%lu)_append\n", cid, classId, versionId);
        ArrangementFilterTask *arrangementFilterTask = new ArrangementFilterTask(true, classId);
        GlobalArrangementFilterPipelinePtr->addTask(arrangementFilterTask);
        return 0;
    }


    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<ArrangementTask *> taskList;
    MutexLock mutexLock;
    Condition condition;

    uint64_t readAmount = 0;
};

static ArrangementReadPipeline* GlobalArrangementReadPipelinePtr;

#endif //MEGA_ARRANGEMENTREADPIPELINE_H
