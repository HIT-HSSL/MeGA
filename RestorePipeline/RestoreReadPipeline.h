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
#include "RestoreParserPipeline.h"

extern std::string ClassFileAppendPath;

struct ReadPos {
    uint64_t offset;
    uint64_t length;
};

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
        return 0;
    }

    ~RestoreReadPipeline() {
        printf("restore read duration :%lu\n", duration);
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:
    void restoreReadCallback() {
        pthread_setname_np(pthread_self(), "Restore Reading Thread");
        RestoreTask *restoreTask;

        struct timeval t0, t1;

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
                    printf("version # %lu is required\n", i);
                }
                uint64_t baseCategory = (restoreTask->maxVersion - 1) * restoreTask->maxVersion / 2 + 1;
                baseClass = baseCategory;
                for (uint64_t i = baseCategory; i < baseCategory + restoreTask->targetVersion; i++) {
                    categoryList.push_front(i);
                    printf("category # %lu is required\n", i);
                }
                printf("append category # %lu is optional\n", baseCategory);
            }else{
                assert(0); // todo: do not consider fall behind currently
                //processing when arrangement falls behind.
                printf("Arrangement falls %lu versions behind\n", restoreTask->fallBehind);
                // read the last version in previous OPT layout
                printf("Load the last version in existing OPT layout..\n");
                for (uint64_t i = restoreTask->targetVersion; i <= restoreTask->maxVersion - 1 - restoreTask->fallBehind; i++) {
                    volumeList.push_back(i);
                    printf("version # %lu is required\n", i);
                }
                baseClass = (restoreTask->maxVersion - 1 - restoreTask->fallBehind) * (restoreTask->maxVersion - restoreTask->fallBehind) / 2 + 1;
                for (uint64_t i = baseClass; i < baseClass + restoreTask->targetVersion - restoreTask->fallBehind; i++) {
                    categoryList.push_back(i);
                    printf("category # %lu is required\n", i);
                }
                printf("append category # %lu is optional\n", baseClass);
                // read unique chunks of following versions.
                printf("The new categories of following versions..\n");
                for (uint64_t i = restoreTask->maxVersion - restoreTask->fallBehind + 1;
                     i <= restoreTask->maxVersion; i++) {
                    categoryList.push_back(i * (i + 1) / 2);
                    printf("category # %lu is required\n", i * (i + 1) / 2);
                }
            }

            for (auto &item : volumeList) {
                readFromVolumeFile(item, restoreTask->targetVersion);
            }

            for (auto &item : categoryList) {
                if (item == baseClass) {
                    readFromAppendCategoryFile(baseClass);
                }
                readFromCategoryFile(item);
            }



            RestoreParseTask *restoreParseTask = new RestoreParseTask(true);
            GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);

            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec - t0.tv_usec;

        }
    }

    int readFromVolumeFile(uint64_t versionId, uint64_t restoreVersion) {
        sprintf(filePath, VersionFilePath.data(), versionId);
        FileOperator versionReader(filePath, FileOpenType::Read);

        uint64_t leftLength = 0;
        uint8_t *readBuffer = (uint8_t *) malloc(FLAGS_RestoreReadBufferLength);
        uint64_t bytesForHeaderLength = versionReader.read(readBuffer, sizeof(VolumeFileHeader));
        uint64_t headerItemCount = ((VolumeFileHeader *) readBuffer)->offsetCount;
        uint64_t totalHeaderLength = sizeof(VolumeFileHeader) + headerItemCount * sizeof(uint64_t);
        uint64_t bytesForHeader = versionReader.read(readBuffer, headerItemCount * sizeof(uint64_t));
        uint64_t *offset = (uint64_t *) (readBuffer);
        std::list<ReadPos> categoryList;
        leftLength += totalHeaderLength;
        for (int i = 0; i < restoreVersion; i++) {
            categoryList.push_front({leftLength, offset[i]});
            leftLength += offset[i];
        }

      for (auto iter = categoryList.begin(); iter != categoryList.end(); iter++) {
        uint8_t *ptr = (uint8_t *) malloc(iter->length);
        versionReader.pread(ptr, iter->offset, iter->length);
        RestoreParseTask *restoreParseTask = new RestoreParseTask(ptr, iter->length);
        restoreParseTask->index = versionId;
        GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);
      }

      free(readBuffer);

      return 0;
    }


    int readFromCategoryFile(uint64_t classId) {
        sprintf(filePath, ClassFilePath.data(), classId);
        FileOperator classReader(filePath, FileOpenType::Read);

        uint64_t leftLength = FileOperator::size(filePath);

        uint8_t *readBuffer = (uint8_t *) malloc(leftLength);
        classReader.read(readBuffer, leftLength);
        RestoreParseTask *restoreParseTask = new RestoreParseTask(readBuffer, leftLength);
        restoreParseTask->index = classId;
        GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);
      return 0;
    }

    int readFromAppendCategoryFile(uint64_t classId) {
        printf("Trying to load append file.\n");
        sprintf(filePath, ClassFileAppendPath.data(), classId);
        FileOperator classReader(filePath, FileOpenType::Read);
        if(classReader.ok()) {
            int fd = classReader.getFd();

            uint64_t leftLength = FileOperator::size(filePath);
            uint8_t *readBuffer = (uint8_t *) malloc(leftLength);
            classReader.read(readBuffer, leftLength);
            RestoreParseTask *restoreParseTask = new RestoreParseTask(readBuffer, leftLength);
            restoreParseTask->index = classId;
            GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);
        }else{
            printf("Append file not exists, ignore it.\n");
        }
      return 0;
    }


    char filePath[256];
    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<RestoreTask *> taskList;
    MutexLock mutexLock;
    Condition condition;

    uint64_t duration = 0;
};

static RestoreReadPipeline *GlobalRestoreReadPipelinePtr;


#endif //MEGA_RESTOREREADPIPELINE_H
