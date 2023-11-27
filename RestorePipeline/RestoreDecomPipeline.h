//
// Created by Borelset on 2021/12/13.
//

#ifndef MEGA_RESTOREDECOMPRESSION_H
#define MEGA_RESTOREDECOMPRESSION_H

#include "RestoreParserPipeline.h"

class RestoreDecomPipeline {
public:
    RestoreDecomPipeline() : taskAmount(0), runningFlag(true), mutexLock(),
                             condition(mutexLock) {
        worker = new std::thread(std::bind(&RestoreDecomPipeline::restoreDecompressionCallback, this));
    }

    int addTask(RestoreParseTask *restoreTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(restoreTask);
        taskAmount++;
        condition.notify();
        return 0;
    }

    ~RestoreDecomPipeline() {
        printf("[RestoreDecom] total: %lu\n", duration);
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:
    void restoreDecompressionCallback() {
        pthread_setname_np(pthread_self(), "RDecom");

        RestoreParseTask *restoreParseTask;

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
                restoreParseTask = taskList.front();
                taskList.pop_front();
            }

            if (restoreParseTask->endFlag) {
                GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);
                continue;
            }

            gettimeofday(&t0, NULL);
            uint8_t *decomBuffer = (uint8_t *) malloc(RestoreReadBufferLength);
            size_t decompressedSize = ZSTD_decompress(decomBuffer, RestoreReadBufferLength, restoreParseTask->buffer,
                                                      restoreParseTask->length);
            assert(!ZSTD_isError(decompressedSize));
            free(restoreParseTask->buffer);
            restoreParseTask->buffer = decomBuffer;
            restoreParseTask->length = decompressedSize;

            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;

            GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);
        }
    }


    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<RestoreParseTask *> taskList;
    MutexLock mutexLock;
    Condition condition;

    uint64_t duration = 0;
};

static RestoreDecomPipeline *GlobalRestoreDecomPipelinePtr;

#endif //MEGA_RESTOREDECOMPRESSION_H
