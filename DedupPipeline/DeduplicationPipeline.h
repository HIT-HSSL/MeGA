/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_DEDUPLICATIONPIPELINE_H
#define MEGA_DEDUPLICATIONPIPELINE_H


#include "jemalloc/jemalloc.h"
#include "../MetadataManager/MetadataManager.h"
#include "WriteFilePipeline.h"
#include <assert.h>
#include "../Utility/Likely.h"
#include "../Utility/xdelta3.h"
#include "../Utility/BaseCache.h"

struct BaseChunkPositions {
    uint64_t category: 22;
    uint64_t quantizedOffset: 42;
};

DEFINE_uint64(CappingThreshold,
              10, "CappingThreshold");

extern bool DeltaSwitch;

class DeduplicationPipeline {
public:
    DeduplicationPipeline()
            : taskAmount(0),
              runningFlag(true),
              mutexLock(),
              condition(mutexLock) {
        worker = new std::thread(std::bind(&DeduplicationPipeline::deduplicationWorkerCallback, this));

    }

    int addTask(const DedupTask &dedupTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        receiveList.push_back(dedupTask);
        taskAmount++;
        condition.notifyAll();
    }

    ~DeduplicationPipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
        delete worker;
    }

    void getStatistics() {
        printf("Deduplicating Duration : %lu\n", duration);
        printf("Delta duration : %lu\n", deltaTime);
        printf("Unique:%lu, Internal:%lu, Adjacent:%lu, Delta:%lu\n", chunkCounter[0], chunkCounter[1], chunkCounter[2], chunkCounter[3]);
        printf("xdeltaError:%lu\n", xdeltaError);
        printf("Total Length : %lu, Unique Length : %lu, Adjacent Length: %lu, Delta Reduce: %lu, Dedup Ratio : %f\n",
               totalLength, afterDedupLength, adjacentDuplicates, deltaReduceLength,
               (float) totalLength / (afterDedupLength - deltaReduceLength));
        printf("cut times:%lu, cut length:%lu\n", cutTimes, cutLength);
        printf("Capping Reject:%lu\n", cappingReject);
    }


private:
    void deduplicationWorkerCallback() {
        pthread_setname_np(pthread_self(), "Dedup Thread");

        std::list<WriteTask> saveList;

        std::list<DedupTask> detectList;
        uint64_t segmentLength = 0;
        uint64_t SegmentThreshold = 20 * 1024 * 1024;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) return;
                }
                //printf("get task\n");
                taskAmount = 0;
                taskList.swap(receiveList);
            }

            if (newVersionFlag) {
                for (int i = 0; i < 4; i++) {
                    chunkCounter[i] = 0;
                }
                newVersionFlag = false;
                duration = 0;
                if(!taskList.empty()){
                    baseCache.setCurrentVersion(taskList.begin()->fileID);
                }
            }

            for (const auto &dedupTask : taskList) {
                detectList.push_back(dedupTask);
                segmentLength += dedupTask.length;
                if (segmentLength > SegmentThreshold || dedupTask.countdownLatch) {

                    processingWaitingList(detectList);
                    cappingBaseChunks(detectList);
                    doDedup(detectList);

                    segmentLength = 0;
                    detectList.clear();
                }
            }
            taskList.clear();
        }

    }

    void processingWaitingList(std::list<DedupTask> &dl) {
        BasePos tempBasePos;
        BlockEntry tempBlockEntry;
        for (auto &entry: dl) {

            FPTableEntry fpTableEntry;
            LookupResult lookupResult = GlobalMetadataManagerPtr->dedupLookup(entry.fp, entry.length, &fpTableEntry);

            entry.lookupResult = lookupResult;

            if (lookupResult == LookupResult::Unique) {
                LookupResult similarLookupResult = LookupResult::Dissimilar;
                odessCalculation(entry.buffer + entry.pos, entry.length, &entry.similarityFeatures);
                if (DeltaSwitch) {
                    similarLookupResult = GlobalMetadataManagerPtr->similarityLookupSimple(entry.similarityFeatures,
                                                                                           &tempBasePos);
                }
                if (similarLookupResult == LookupResult::Similar) {
                    int r = baseCache.getRecord(&tempBasePos, &tempBlockEntry);
                    entry.lookupResult = similarLookupResult;
                    entry.basePos = tempBasePos;
                    entry.inCache = r;

                } else {
                    // unique
                    // do nothing
                }
            } else if (lookupResult == LookupResult::InternalDedup) {
                // do nothing
            } else if (lookupResult == LookupResult::InternalDeltaDedup) {
                // do nothing
            } else if (lookupResult == LookupResult::AdjacentDedup) {
                // do nothing
            }
        }
    }

    void cappingBaseChunks(std::list<DedupTask> &dl) {
        std::unordered_map<uint64_t, uint64_t> baseChunkPositions;
        for (auto &entry: dl) {
            if (entry.lookupResult == LookupResult::Similar && entry.inCache == 0) {
                uint64_t key;
                BaseChunkPositions *bcp = (BaseChunkPositions *) &key;
                bcp->category = entry.basePos.CategoryOrder;
                bcp->quantizedOffset = entry.basePos.cid;
                auto iter = baseChunkPositions.find(key);
                if (iter == baseChunkPositions.end()) {
                    baseChunkPositions.insert({key, 1});
                } else {
                    baseChunkPositions[key]++;
                }
            }
        }
        for (auto &entry: baseChunkPositions) {
            if (entry.second < FLAGS_CappingThreshold) {
                entry.second = 0;
            }
        }
        for (auto &entry: dl) {
            if (entry.lookupResult == LookupResult::Similar && entry.inCache == 0) {
                uint64_t key;
                BaseChunkPositions *bcp = (BaseChunkPositions *) &key;
                bcp->category = entry.basePos.CategoryOrder;
                bcp->quantizedOffset = entry.basePos.cid;
                if (baseChunkPositions[key] == 0) {
                    entry.deltaReject = true;
                }
            }
        }

    }

    void doDedup(std::list<DedupTask> &dl) {
        WriteTask writeTask;
        BlockEntry tempBlockEntry;
        struct timeval t0, t1, dt1, dt2;

        for (auto &entry: dl) {
            gettimeofday(&t0, NULL);
            memset(&writeTask, 0, sizeof(WriteTask));

            FPTableEntry fpTableEntry;
            LookupResult lookupResult = GlobalMetadataManagerPtr->dedupLookup(entry.fp, entry.length, &fpTableEntry);

            writeTask.fileID = entry.fileID;
            writeTask.index = entry.index;
            writeTask.buffer = entry.buffer;
            writeTask.pos = entry.pos;
            writeTask.length = entry.length;
            writeTask.sha1Fp = entry.fp;
            writeTask.deltaTag = 0;

            totalLength += entry.length;

            writeTask.type = (int) lookupResult;

            if (lookupResult == LookupResult::Unique) {
                chunkCounter[(int) lookupResult]++;
                LookupResult similarLookupResult = LookupResult::Dissimilar;
                if (DeltaSwitch) {
                    similarLookupResult = GlobalMetadataManagerPtr->similarityLookupSimple(entry.similarityFeatures,
                                                                                           &entry.basePos);
                }
                if (similarLookupResult == LookupResult::Similar && !entry.deltaReject) {
                    lookupResult = LookupResult::Dissimilar;
                    int r;
                    r = baseCache.getRecordWithoutFresh(&entry.basePos, &tempBlockEntry);
                    if (!r) {
                        baseCache.loadBaseChunks(entry.basePos);
                        r = baseCache.getRecordNoFS(&entry.basePos, &tempBlockEntry);
                        assert(r);
                    }

                    // calculate delta
                    uint8_t *tempBuffer = (uint8_t *) malloc(65536);
                    usize_t deltaSize;
                    gettimeofday(&dt1, NULL);

                    r = xd3_encode_memory(entry.buffer + entry.pos, entry.length,
                                          tempBlockEntry.block, tempBlockEntry.length, tempBuffer, &deltaSize,
                                          entry.length, XD3_COMPLEVEL_1);
                    gettimeofday(&dt2, NULL);
                    deltaTime += (dt2.tv_sec - dt1.tv_sec) * 1000000 + dt2.tv_usec - dt1.tv_usec;

                    if (r != 0 || deltaSize >= entry.length) {
                        // no delta
                        free(tempBuffer);
                        xdeltaError++;
                        goto unique;
                    } else {
                        // add metadata
                        GlobalMetadataManagerPtr->deltaAddRecord(writeTask.sha1Fp, entry.fileID,
                                                                 entry.basePos.sha1Fp,
                                                                 entry.length - deltaSize,
                                                                 entry.length);
                        // extend base lifecycle
                        FPTableEntry tFTE = {
                                0,
                                entry.basePos.CategoryOrder,
                                entry.basePos.length,
                                entry.basePos.length
                        };
                        GlobalMetadataManagerPtr->extendBase(entry.basePos.sha1Fp, tFTE);
                        // update task
                        writeTask.type = (int) similarLookupResult;
                        writeTask.buffer = tempBuffer;
                        writeTask.pos = 0;
                        writeTask.length = deltaSize;
                        writeTask.oriLength = entry.length;
                        writeTask.deltaTag = 1;
                        writeTask.baseFP = entry.basePos.sha1Fp;
                        deltaReduceLength += entry.length - deltaSize;
                        lastCategoryLength += deltaSize + sizeof(BlockHeader);
                        if (lastCategoryLength >= ContainerSize) {
                            lastCategoryLength = 0;
                            currentCID++;
                        }
                        chunkCounter[3]++;
                    }
                } else {
                    unique:
                    if (entry.deltaReject) cappingReject++;
                    writeTask.type = (int) LookupResult::Unique;
                    GlobalMetadataManagerPtr->uniqueAddRecord(entry.fp, entry.fileID, entry.length);
                    GlobalMetadataManagerPtr->addSimilarFeature(entry.similarityFeatures,
                                                                {entry.fp, (uint32_t) entry.fileID,
                                                                 currentCID, entry.length});
                    writeTask.similarityFeatures = entry.similarityFeatures;
                    baseCache.addRecord(writeTask.sha1Fp, writeTask.buffer + writeTask.pos, writeTask.length);
                    lastCategoryLength += entry.length + sizeof(BlockHeader);
                    if (lastCategoryLength >= ContainerSize) {
                        lastCategoryLength = 0;
                        currentCID++;
                    }
                }
                afterDedupLength += entry.length;
            } else if (lookupResult == LookupResult::InternalDedup) {
                chunkCounter[(int) lookupResult]++;
                // nothing to do
            } else if (lookupResult == LookupResult::InternalDeltaDedup) {
                chunkCounter[(int) LookupResult::InternalDedup]++;
                writeTask.type = (int) LookupResult::InternalDedup;
                writeTask.oriLength = fpTableEntry.oriLength; //updated
                writeTask.baseFP = fpTableEntry.baseFP;
                writeTask.deltaTag = 1;
                writeTask.length = fpTableEntry.length;
            } else if (lookupResult == LookupResult::AdjacentDedup) {
                chunkCounter[(int) lookupResult]++;
                adjacentDuplicates += entry.length;
                GlobalMetadataManagerPtr->neighborAddRecord(writeTask.sha1Fp, fpTableEntry);
                if (fpTableEntry.deltaTag) {
                    writeTask.length = fpTableEntry.length;
                    writeTask.deltaTag = 1;
                    writeTask.baseFP = fpTableEntry.baseFP;
                    writeTask.oriLength = fpTableEntry.oriLength; //updated
                    FPTableEntry tFTE = {
                            0,
                            fpTableEntry.categoryOrder
                    };
                    GlobalMetadataManagerPtr->neighborAddRecord(fpTableEntry.baseFP, tFTE);
                }
            }

            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;

            if (unlikely(entry.countdownLatch)) {
                printf("DedupPipeline finish\n");
                writeTask.countdownLatch = entry.countdownLatch;
                entry.countdownLatch->countDown();
                //GlobalMetadataManagerPtr->tableRolling();
                newVersionFlag = true;

                GlobalWriteFilePipelinePtr->addTask(writeTask);
            } else {
                GlobalWriteFilePipelinePtr->addTask(writeTask);
            }

            writeTask.countdownLatch = nullptr;

        }

    }

    std::thread *worker;
    std::list<DedupTask> taskList;
    std::list<DedupTask> receiveList;
    int taskAmount;
    bool runningFlag;
    MutexLock mutexLock;
    Condition condition;

    BaseCache baseCache;

    uint64_t totalLength = 0;
    uint64_t afterDedupLength = 0;
    uint64_t adjacentDuplicates = 0;
    uint64_t deltaReduceLength = 0;
    uint64_t lastCategoryLength = 0;
    uint64_t currentCID = 0;

    uint64_t chunkCounter[4] = {0, 0, 0, 0};
    uint64_t xdeltaError = 0;

    uint64_t duration = 0;
    uint64_t deltaTime = 0;

    uint64_t cutLength = 0, cutTimes = 0;
    uint64_t cappingReject = 0;

    bool newVersionFlag = true;
};

static DeduplicationPipeline *GlobalDeduplicationPipelinePtr;

#endif //MEGA_DEDUPLICATIONPIPELINE_H
