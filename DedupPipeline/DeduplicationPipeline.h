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
    }


private:
    void deduplicationWorkerCallback() {
        pthread_setname_np(pthread_self(), "Dedup Thread");
        WriteTask writeTask;

        struct timeval t0, t1, dt1, dt2;
        std::list<WriteTask> saveList;
        uint8_t *currentTask;
        bool newVersionFlag = true;

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

            SimilarityFeatures tempSimilarityFeatures;
            BasePos bpResult[6];
            BasePos tempBasePos;
            BlockEntry tempBlockEntry;
            for (const auto &dedupTask : taskList) {

                gettimeofday(&t0, NULL);

                writeTask.fileID = dedupTask.fileID;
                writeTask.index = dedupTask.index;

                FPTableEntry fpTableEntry;
                LookupResult lookupResult = GlobalMetadataManagerPtr->dedupLookup(dedupTask.fp, dedupTask.length, &fpTableEntry);

                writeTask.type = (int) lookupResult;
                writeTask.buffer = dedupTask.buffer;
                writeTask.pos = dedupTask.pos;
                writeTask.length = dedupTask.length;
                writeTask.sha1Fp = dedupTask.fp;
                writeTask.deltaTag = 0;

                totalLength += dedupTask.length;

                if(lookupResult == LookupResult::Unique){
                    chunkCounter[(int) lookupResult]++;
                    odessCalculation(dedupTask.buffer + dedupTask.pos, dedupTask.length, &tempSimilarityFeatures);
                    LookupResult similarLookupResult = GlobalMetadataManagerPtr->similarityLookup(
                            tempSimilarityFeatures, bpResult);
                    if (similarLookupResult == LookupResult::Similar) {
//                        // pick a base
//                        int r = 0;
//                        for (int i = 0; i < 6; i++) {
//                            if (bpResult[i].valid) {
//                                tempBasePos = bpResult[i];
//                                r = baseCache.getRecord(bpResult[i].sha1Fp, &tempBlockEntry);
//                                if (r) break;
//                            }
//                        }
//                        // load from disk
//                        if (!r) {
//                            baseCache.loadBaseChunks(tempBasePos);
//                            r = baseCache.getRecord(tempBasePos.sha1Fp, &tempBlockEntry);
//                            assert(r);
//                        }
                        int r = baseCache.getRecordBatch(bpResult, 6, &tempBlockEntry, &tempBasePos);
                        // calculate delta
                        uint8_t *tempBuffer = (uint8_t *) malloc(dedupTask.length);
                        usize_t deltaSize;
                        gettimeofday(&dt1, NULL);
                        if (tempBlockEntry.length >= dedupTask.length) {
                            r = xd3_encode_memory(dedupTask.buffer + dedupTask.pos, dedupTask.length,
                                                  tempBlockEntry.block, dedupTask.length, tempBuffer, &deltaSize,
                                                  dedupTask.length, XD3_COMPLEVEL_1);
                            cutLength += tempBlockEntry.length - dedupTask.length;
                            cutTimes++;
                        } else {
                            uint8_t *baseBuffer = (uint8_t *) malloc(dedupTask.length);
                            memset(baseBuffer, 0, dedupTask.length);
                            memcpy(baseBuffer, tempBlockEntry.block, tempBlockEntry.length);
                            r = xd3_encode_memory(dedupTask.buffer + dedupTask.pos, dedupTask.length, baseBuffer,
                                                  dedupTask.length, tempBuffer, &deltaSize, dedupTask.length,
                                                  XD3_COMPLEVEL_1);
                            free(baseBuffer);
                        }
                        gettimeofday(&dt2, NULL);
                        deltaTime += (dt2.tv_sec - dt1.tv_sec) * 1000000 + dt2.tv_usec - dt1.tv_usec;

                        if (r != 0 || deltaSize >= dedupTask.length) {
                            // no delta
                            free(tempBuffer);
                            xdeltaError++;
                            goto unique;
                        } else {
                            // add metadata
                            GlobalMetadataManagerPtr->deltaAddRecord(writeTask.sha1Fp, dedupTask.fileID,
                                                                     tempBasePos.sha1Fp,
                                                                     dedupTask.length - deltaSize,
                                                                     dedupTask.length);
                            // extend base lifecycle
                            GlobalMetadataManagerPtr->extendBase(tempBasePos.sha1Fp,
                                                                 {0, tempBasePos.CategoryOrder, tempBasePos.length});
                            // update task
                            writeTask.type = (int) similarLookupResult;
                            writeTask.buffer = tempBuffer;
                            writeTask.pos = 0;
                            writeTask.length = deltaSize;
                            writeTask.oriLength = dedupTask.length;
                            writeTask.deltaTag = 1;
                            writeTask.baseFP = tempBasePos.sha1Fp;
                            deltaReduceLength += dedupTask.length - deltaSize;
                            lastCategoryLength += deltaSize + sizeof(BlockHeader);
                            chunkCounter[3]++;
                        }
                    } else {
                        unique:
                        GlobalMetadataManagerPtr->uniqueAddRecord(writeTask.sha1Fp, dedupTask.fileID, dedupTask.length);
                        GlobalMetadataManagerPtr->addSimilarFeature(tempSimilarityFeatures,
                                                                    {writeTask.sha1Fp, (uint32_t) dedupTask.fileID,
                                                                     lastCategoryLength, dedupTask.length});
                        writeTask.similarityFeatures = tempSimilarityFeatures;
                        baseCache.addRecord(writeTask.sha1Fp, writeTask.buffer + writeTask.pos, writeTask.length);
                        lastCategoryLength += dedupTask.length + sizeof(BlockHeader);
                    }
                    afterDedupLength += dedupTask.length;
                }
                else if(lookupResult == LookupResult::InternalDedup){
                    chunkCounter[(int) lookupResult]++;
                    // nothing to do
                }
                else if(lookupResult == LookupResult::InternalDeltaDedup) {
                    chunkCounter[(int) LookupResult::InternalDedup]++;
                    writeTask.type = (int) LookupResult::InternalDedup;
                    writeTask.oriLength = fpTableEntry.oriLength; //updated
                    writeTask.baseFP = fpTableEntry.baseFP;
                    writeTask.deltaTag = 1;
                }
                else if(lookupResult == LookupResult::AdjacentDedup){
                    chunkCounter[(int) lookupResult]++;
                    adjacentDuplicates += dedupTask.length;
                    GlobalMetadataManagerPtr->neighborAddRecord(writeTask.sha1Fp, fpTableEntry);
                    if(fpTableEntry.deltaTag) {
                        writeTask.deltaTag = 1;
                        writeTask.baseFP = fpTableEntry.baseFP;
                        writeTask.oriLength = fpTableEntry.oriLength; //updated
                        GlobalMetadataManagerPtr->neighborAddRecord(fpTableEntry.baseFP,
                                                                    {0, fpTableEntry.categoryOrder});
                    }
                }

                gettimeofday(&t1, NULL);
                duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;

                if (unlikely(dedupTask.countdownLatch)) {
                    printf("DedupPipeline finish\n");
                    writeTask.countdownLatch = dedupTask.countdownLatch;
                    dedupTask.countdownLatch->countDown();
                    //GlobalMetadataManagerPtr->tableRolling();
                    newVersionFlag = true;

                    GlobalWriteFilePipelinePtr->addTask(writeTask);
                } else {
                    GlobalWriteFilePipelinePtr->addTask(writeTask);
                }

                writeTask.countdownLatch = nullptr;
            }
            taskList.clear();
        }

    }

    std::thread *worker;
    std::list <DedupTask> taskList;
    std::list <DedupTask> receiveList;
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

    uint64_t chunkCounter[4] = {0, 0, 0, 0};
    uint64_t xdeltaError = 0;

    uint64_t duration = 0;
    uint64_t deltaTime = 0;

    uint64_t cutLength = 0, cutTimes = 0;


};

static DeduplicationPipeline *GlobalDeduplicationPipelinePtr;

#endif //MEGA_DEDUPLICATIONPIPELINE_H
