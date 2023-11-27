/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_RESTOREPARSERPIPELINE_H
#define MEGA_RESTOREPARSERPIPELINE_H

#include "RestoreWritePipeline.h"
#include "../Utility/StorageTask.h"
#include "../Utility/FileOperator.h"
#include <thread>
#include <assert.h>

extern uint64_t ContainerSize;
uint64_t RestoreReadBufferLength = ContainerSize * 1.2;

struct BlockRestorePos {
    uint64_t offset;
};

class RestoreParserPipeline {
public:
    RestoreParserPipeline(uint64_t target, const std::string &path) : taskAmount(0), runningFlag(true), mutexLock(),
                                                                      condition(mutexLock) {
        worker = new std::thread(std::bind(&RestoreParserPipeline::restoreParserCallback, this, path));
    }

    int addTask(RestoreParseTask *restoreParseTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(restoreParseTask);
        taskAmount++;
        condition.notify();
        return 0;
    }

    ~RestoreParserPipeline() {
        printf("[RestoreParser] total :%lu\n", duration);
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:
    void restoreParserCallback(const std::string &path) {
        pthread_setname_np(pthread_self(), "RParsing");

        FileOperator recipeFD((char *) path.data(), FileOpenType::Read);
        uint64_t size = FileOperator::size(path);
        uint8_t *recipeBuffer = (uint8_t *) malloc(size);
        recipeFD.read(recipeBuffer, size);
        uint64_t count = size / sizeof(BlockHeader);
        assert(count * sizeof(BlockHeader) == size);
        BlockHeader *blockHeader;
        printf("Chunks:%lu\n", count);

        uint64_t pos = 0;
        for (int i = 0; i < count; i++) {
            blockHeader = (BlockHeader *) (recipeBuffer + i * sizeof(BlockHeader));
            if (blockHeader->type) {
                restoreMap[blockHeader->baseFP].push_back({0, 1, pos, blockHeader->length});
                restoreMap[blockHeader->fp].push_back({1, 0, pos, 0});
                pos += blockHeader->oriLength;
            } else {
                restoreMap[blockHeader->fp].push_back({0, 0, pos, 0});
                pos += blockHeader->length;
            }
        }
        printf("total size:%lu\n", pos);
        GlobalRestoreWritePipelinePtr->setSize(pos);

        RestoreParseTask *restoreParseTask;

        struct timeval t0, t1;

        uint64_t readLength = 0;

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

            gettimeofday(&t0, NULL);

            if (unlikely(restoreParseTask->endFlag)) {
                printf("Read amplification : %f\n", (float) readLength / pos);
                delete restoreParseTask;
                RestoreWriteTask *restoreWriteTask = new RestoreWriteTask(true);
                GlobalRestoreWritePipelinePtr->addTask(restoreWriteTask);
                gettimeofday(&t1, NULL);
                duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
                break;
            }

            std::list<BlockRestorePos> orderList;
            uint64_t leftLength = restoreParseTask->length;
            uint64_t readoffset = 0;
            uint8_t *buffer = restoreParseTask->buffer;

            readLength += restoreParseTask->sizeAfterCompression;

            while (readoffset < leftLength) {
                BlockHeader *pBH = (BlockHeader *) (buffer + readoffset);
                assert(leftLength > sizeof(BlockHeader));
                assert(leftLength >= sizeof(BlockHeader) + pBH->length);
                if (pBH->type) {
                    orderList.push_front({readoffset});
                } else {
                    orderList.push_back({readoffset});
                }
                readoffset += sizeof(BlockHeader) + pBH->length;
            }
            assert(readoffset == leftLength);

            for (const auto &entry: orderList) {
                BlockHeader *pBH = (BlockHeader *) (buffer + entry.offset);
                uint8_t *bufferPtr = (uint8_t *) (buffer + entry.offset + sizeof(BlockHeader));
                auto iter = restoreMap.find(pBH->fp);
                // if we allow arrangement to fall behind, below assert must be commented.
                assert(iter->second.size() > 0);
                if (iter != restoreMap.end()) {
                    for (auto item: iter->second) {
                        totalLength += pBH->length;
                        // item.length could be the length before delta (not the actual delta size), when delta chunk is migrated as adjacent.
                        RestoreWriteTask *restoreWriteTask = new RestoreWriteTask(bufferPtr, item.pos,
                                                                                  pBH->length, item.type, item.base,
                                                                                  item.deltaLength);
                        GlobalRestoreWritePipelinePtr->addTask(restoreWriteTask);
                    }
                } else {
                    assert(0);
                }
            }

            delete restoreParseTask;
            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
        }
    }

    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<RestoreParseTask *> taskList;
    MutexLock mutexLock;
    Condition condition;

    uint64_t totalLength = 0;

    std::unordered_map<SHA1FP, std::list<RestoreMapListEntry>, TupleHasher, TupleEqualer> restoreMap;

    uint64_t duration = 0;
};

static RestoreParserPipeline *GlobalRestoreParserPipelinePtr;

#endif //MEGA_RESTOREPARSERPIPELINE_H
