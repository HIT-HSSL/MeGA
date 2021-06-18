/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_WRITEFILEPIPELINE_H
#define MEGA_WRITEFILEPIPELINE_H


#include "jemalloc/jemalloc.h"
#include "../MetadataManager/MetadataManager.h"
#include "../Utility/ChunkWriterManager.h"
#include "../Utility/Likely.h"
#include "../Utility/BufferedFileWriter.h"

extern std::string LogicFilePath;

DEFINE_uint64(RecipeFlushBufferSize,
              8388608, "RecipeFlushBufferSize");

class WriteFilePipeline {
public:
    WriteFilePipeline() : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock),
                          logicFileOperator(nullptr) {
        worker = new std::thread(std::bind(&WriteFilePipeline::writeFileCallback, this));
    }

    int addTask(const WriteTask &writeTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        receiveList.push_back(writeTask);
        taskAmount++;
        condition.notify();
    }

    ~WriteFilePipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
        delete worker;
    }

    void getStatistics() {
        printf("Write duration:%lu\n", duration);
    }

private:
    void writeFileCallback() {
        pthread_setname_np(pthread_self(), "Writing Thread");
        struct timeval t0, t1;
        bool newVersionFlag = true;

        BlockHeader blockHeader;
        ChunkWriterManager *chunkWriterManager = nullptr;
        uint64_t recipeLength = 0;

        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) return;
                }
                taskAmount = 0;
                condition.notify();
                taskList.swap(receiveList);
            }

            gettimeofday(&t0, NULL);

            if (chunkWriterManager == nullptr) {
                chunkWriterManager = new ChunkWriterManager(TotalVersion);
                duration = 0;
            }

            for (auto &writeTask : taskList) {

                if (!logicFileOperator) {
                    sprintf(buffer, LogicFilePath.c_str(), writeTask.fileID);
                    logicFileOperator = new FileOperator(buffer, FileOpenType::Write);
                    bufferedFileWriter = new BufferedFileWriter(logicFileOperator, FLAGS_RecipeFlushBufferSize, 1);
                    printf("start write\n");
                }
                blockHeader = {
                        writeTask.sha1Fp,
                        writeTask.deltaTag,
                        writeTask.length,
                };
                switch (writeTask.type) {
                    case 0: //Unique
                        blockHeader.sFeatures = writeTask.similarityFeatures;
                        chunkWriterManager->writeClass((uint8_t * ) & blockHeader, sizeof(BlockHeader),
                                                       writeTask.buffer + writeTask.pos, writeTask.length);
                        bufferedFileWriter->write((uint8_t * ) & blockHeader, sizeof(BlockHeader));
                        recipeLength += blockHeader.length;
                        break;
                    case 1: //Internal
                        if(writeTask.deltaTag){
                            blockHeader.baseFP = writeTask.baseFP;
                            blockHeader.oriLength = writeTask.oriLength;
                        }
                        bufferedFileWriter->write((uint8_t * ) & blockHeader, sizeof(BlockHeader));
                        recipeLength += blockHeader.length;
                        break;
                    case 2: //Adjacent
                        if(writeTask.deltaTag){
                            blockHeader.baseFP = writeTask.baseFP;
                            blockHeader.oriLength = writeTask.oriLength;
                        }
                        bufferedFileWriter->write((uint8_t * ) & blockHeader, sizeof(BlockHeader));
                        recipeLength += blockHeader.length;
                        break;
                    case 4: //Similar
                        blockHeader.baseFP = writeTask.baseFP;
                        blockHeader.oriLength = writeTask.oriLength;
                        chunkWriterManager->writeClass((uint8_t * ) & blockHeader, sizeof(BlockHeader),
                                                       writeTask.buffer, writeTask.length);
                        bufferedFileWriter->write((uint8_t * ) & blockHeader, sizeof(BlockHeader));
                        recipeLength += blockHeader.oriLength;
                        free(writeTask.buffer);
                        break;
                    default:
                        assert(1);
                        break;
                }

                if (writeTask.countdownLatch) {
                    printf("WritePipeline finish\n");
                    delete bufferedFileWriter;
                    delete logicFileOperator;
                    logicFileOperator = nullptr;
                    delete chunkWriterManager;
                    chunkWriterManager = nullptr;
                    gettimeofday(&t1, NULL);
                    duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;

                    writeTask.countdownLatch->countDown();
                    free(writeTask.buffer);
                }

            }
            taskList.clear();

            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
        }
    }

    FileOperator *logicFileOperator;
    BufferedFileWriter* bufferedFileWriter;
    char buffer[256];
    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list <WriteTask> taskList;
    std::list <WriteTask> receiveList;
    MutexLock mutexLock;
    Condition condition;
    uint64_t duration = 0;

};

static WriteFilePipeline *GlobalWriteFilePipelinePtr;

#endif //MEGA_WRITEFILEPIPELINE_H
