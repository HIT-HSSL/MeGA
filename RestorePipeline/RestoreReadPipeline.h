/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_RESTOREREADPIPELINE_H
#define MEGA_RESTOREREADPIPELINE_H

#include <fcntl.h>
#include "RestoreDecomPipeline.h"

extern std::string ClassFileAppendPath;

struct ReadPos {
    uint64_t offset;
    uint64_t length;
};

struct timeval t0, t1, rt0, rt1;

class RestoreReadPipeline {
public:
    RestoreReadPipeline() : taskAmount(0), runningFlag(true), mutexLock(),
                            condition(mutexLock) {
        worker = new std::thread(std::bind(&RestoreReadPipeline::restoreReadCallback, this));
    }

    int addTask(RestoreTask *restoreTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(restoreTask);
        taskAmount++;
        condition.notify();
    }

    ~RestoreReadPipeline() {
        printf("[RestoreRead] total: %lu, read time:%lu\n", duration, readTime);
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:
    void restoreReadCallback() {
        pthread_setname_np(pthread_self(), "RReading");
        RestoreTask *restoreTask;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                restoreTask = taskList.front();
                taskList.pop_front();
            }
            gettimeofday(&t0, NULL);

            uint64_t baseClass = 0;
            std::list<uint64_t> categoryList, volumeList;
            if(restoreTask->fallBehind == 0 || (restoreTask->maxVersion - restoreTask->fallBehind) >= restoreTask->targetVersion){
                for (uint64_t i = restoreTask->targetVersion; i < restoreTask->maxVersion; i++) {
                    volumeList.push_back(i);
                    printf("Column # %lu is required\n", i);
                }
                uint64_t baseCategory = 1;
                baseClass = baseCategory;
                for (uint64_t i = baseCategory; i < baseCategory + restoreTask->targetVersion; i++) {
                    categoryList.push_front(i);
                    printf("LC-group # %lu is required\n", i);
                }
                printf("append LC-group # %lu is optional\n", baseCategory);
            }else{
                assert(0); // todo: do not consider fall behind currently
            }

            for (auto &item : volumeList) {
                readFromVolumeFile(item, restoreTask->targetVersion);
            }

            for (auto &item : categoryList) {
                if (item == baseClass) {
                    readFromAppendCategoryFile(baseClass, restoreTask->maxVersion);
                }
                readFromCategoryFile(item, restoreTask->maxVersion);
            }


            printf("read done\n");
            RestoreParseTask *restoreParseTask = new RestoreParseTask(true);
            GlobalRestoreDecomPipelinePtr->addTask(restoreParseTask);

            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec - t0.tv_usec;

        }
    }

    int readFromVolumeFile(uint64_t versionId, uint64_t restoreVersion) {
        for (int i = restoreVersion; i >= 1; i--) {
            uint64_t cid = 0;
            while (1) {
                sprintf(filePath, VersionFilePath.data(), i, versionId, cid);
                FileOperator archivedReader(filePath, FileOpenType::TRY);
                if (!archivedReader.ok()) {
                    break;
                }
                cid++;
            }
            uint64_t cidMax = cid - 1;

            for (int j = cidMax; j >= 0; j--) {
                sprintf(filePath, VersionFilePath.data(), i, versionId, j);
                FileOperator archivedReader(filePath, FileOpenType::Read);
                uint8_t *readBuffer = (uint8_t *) malloc(RestoreReadBufferLength);
                gettimeofday(&rt0, NULL);
                uint64_t readLength = archivedReader.read(readBuffer, RestoreReadBufferLength);
                gettimeofday(&rt1, NULL);
                readTime += (rt1.tv_sec - rt0.tv_sec) * 1000000 + rt1.tv_usec - rt0.tv_usec;;

//                uint8_t *decompressBuffer = (uint8_t *) malloc(RestoreReadBufferLength);
//                gettimeofday(&dt0, NULL);
//                size_t decompressedSize = ZSTD_decompress(decompressBuffer, RestoreReadBufferLength, readBuffer,
//                                                          readLength);
//                gettimeofday(&dt1, NULL);
//                decompressionTime += (dt1.tv_sec - dt0.tv_sec) * 1000000 + dt1.tv_usec - dt0.tv_usec;;
//                assert(!ZSTD_isError(decompressedSize));
//                free(readBuffer);

                RestoreParseTask *restoreParseTask = new RestoreParseTask(readBuffer, readLength,
                                                                          readLength);
                restoreParseTask->index = versionId;
                GlobalRestoreDecomPipelinePtr->addTask(restoreParseTask);
            }
        }

    }


    int readFromCategoryFile(uint64_t classId, uint64_t column) {
        uint64_t cid = 0;
        while (1) {
            sprintf(filePath, ClassFilePath.data(), classId, column, cid);
            FileOperator activeReader(filePath, FileOpenType::TRY);
            if (!activeReader.ok()) {
                break;
            }
            cid++;
        }
        uint64_t cidMax = cid - 1;

        for (int j = cidMax; j >= 0; j--) {
            sprintf(filePath, ClassFilePath.data(), classId, column, j);
            FileOperator activeReader(filePath, FileOpenType::Read);
            uint8_t *readBuffer = (uint8_t *) malloc(RestoreReadBufferLength);
            gettimeofday(&rt0, NULL);
            uint64_t readLength = activeReader.read(readBuffer, RestoreReadBufferLength);
            gettimeofday(&rt1, NULL);
            readTime += (rt1.tv_sec - rt0.tv_sec) * 1000000 + rt1.tv_usec - rt0.tv_usec;;

//            uint8_t *decompressBuffer = (uint8_t *) malloc(RestoreReadBufferLength);
//            gettimeofday(&dt0, NULL);
//            size_t decompressedSize = ZSTD_decompress(decompressBuffer, RestoreReadBufferLength, readBuffer,
//                                                      readLength);
//            gettimeofday(&dt1, NULL);
//            decompressionTime += (dt1.tv_sec - dt0.tv_sec) * 1000000 + dt1.tv_usec - dt0.tv_usec;;
//            assert(!ZSTD_isError(decompressedSize));
//            free(readBuffer);

            RestoreParseTask *restoreParseTask = new RestoreParseTask(readBuffer, readLength, readLength);
            restoreParseTask->index = column;
            GlobalRestoreDecomPipelinePtr->addTask(restoreParseTask);
        }
    }

    int readFromAppendCategoryFile(uint64_t classId, uint64_t column) {
        printf("Trying to load append file.\n");

        uint64_t cid = 0;
        while (1) {
            sprintf(filePath, ClassFileAppendPath.data(), classId, column, cid);
            FileOperator activeReader(filePath, FileOpenType::TRY);
            if (!activeReader.ok()) {
                break;
            }
            cid++;
        }
        uint64_t cidMax = cid - 1;

        for (int j = cidMax; j >= 0; j--) {
            sprintf(filePath, ClassFileAppendPath.data(), classId, column, j);
            FileOperator activeReader(filePath, FileOpenType::Read);
            uint8_t *readBuffer = (uint8_t *) malloc(RestoreReadBufferLength);
            gettimeofday(&rt0, NULL);
            uint64_t readLength = activeReader.read(readBuffer, RestoreReadBufferLength);
            gettimeofday(&rt1, NULL);
            readTime += (rt1.tv_sec - rt0.tv_sec) * 1000000 + rt1.tv_usec - rt0.tv_usec;;

//            uint8_t *decompressBuffer = (uint8_t *) malloc(RestoreReadBufferLength);
//            gettimeofday(&dt0, NULL);
//            size_t decompressedSize = ZSTD_decompress(decompressBuffer, RestoreReadBufferLength, readBuffer,
//                                                      readLength);
//            gettimeofday(&dt1, NULL);
//            decompressionTime += (dt1.tv_sec - dt0.tv_sec) * 1000000 + dt1.tv_usec - dt0.tv_usec;;
//            assert(!ZSTD_isError(decompressedSize));
//            free(readBuffer);

            RestoreParseTask *restoreParseTask = new RestoreParseTask(readBuffer, readLength, readLength);
            restoreParseTask->index = column;
            GlobalRestoreDecomPipelinePtr->addTask(restoreParseTask);
        }
    }


    char filePath[256];
    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<RestoreTask *> taskList;
    MutexLock mutexLock;
    Condition condition;

    uint64_t readTime = 0;

    uint64_t duration = 0;
};

static RestoreReadPipeline *GlobalRestoreReadPipelinePtr;


#endif //MEGA_RESTOREREADPIPELINE_H
