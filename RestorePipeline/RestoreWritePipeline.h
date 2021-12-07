/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_RESTOREWRITEPIPELINE_H
#define MEGA_RESTOREWRITEPIPELINE_H

#include <zstd.h>

#define ChunkBufferSize 65536

class FileFlusher {
public:
    FileFlusher(FileOperator *f) : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock),
                                   fileOperator(f) {
        worker = new std::thread(std::bind(&FileFlusher::fileFlusherCallback, this));
    }

    int addTask(uint64_t task) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(task);
        taskAmount++;
        condition.notify();
    }

    ~FileFlusher(){
        addTask(-1);
        worker->join();
    }


private:

    void fileFlusherCallback(){
        pthread_setname_np(pthread_self(), "Restore Flushing Thread");
        uint64_t task;
        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                task = taskList.front();
                taskList.pop_front();
            }

            if(task == -1){
                break;
            }

            fileOperator->fdatasync();
        }
    }

    std::thread* worker;
    bool runningFlag;
    uint64_t taskAmount;
    std::list<uint64_t> taskList;
    MutexLock mutexLock;
    Condition condition;
    FileOperator* fileOperator;
};

class RestoreWritePipeline {
public:
    RestoreWritePipeline(std::string restorePath, CountdownLatch *cd) : taskAmount(0), runningFlag(true), mutexLock(),
                                               condition(mutexLock), countdownLatch(cd) {
        fileOperator = new FileOperator((char*)restorePath.data(), FileOpenType::Write);
        worker = new std::thread(std::bind(&RestoreWritePipeline::restoreWriteCallback, this));
    }

    int addTask(RestoreWriteTask *restoreWriteTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(restoreWriteTask);
        taskAmount++;
        condition.notify();
    }

    ~RestoreWritePipeline() {
        printf("write amplification: %f (%lu / %lu Bytes)\n", (float) extraIO / normalIO, extraIO, normalIO);
        printf("restore write duration :%lu us\n", duration);
        printf("extra read time:%lu us, decoding time:%lu us\n", readTime, decodingTime);
        printf("total chunks:%lu, delta chunks:%lu\n", chunkCounter, deltaCounter);
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    int setSize(uint64_t size){
        totalSize = size;
        if (fileOperator){
            fileOperator->trunc(size);
        }
    }

    uint64_t getTotalSize(){
        return totalSize;
    }

private:
    void restoreWriteCallback() {
        pthread_setname_np(pthread_self(), "RWriting");
        RestoreWriteTask *restoreWriteTask;
        int fd = fileOperator->getFd();
        FileFlusher fileFlusher(fileOperator);
        uint8_t *deltaBuffer = (uint8_t *) malloc(ChunkBufferSize);
        uint8_t *oriBuffer = (uint8_t *) malloc(ChunkBufferSize);
        usize_t oriSize = 0;
        struct timeval t0, t1, dt1, dt2, rt1, rt2;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                restoreWriteTask = taskList.front();
                taskList.pop_front();
            }
            gettimeofday(&t0, NULL);

            if (unlikely(restoreWriteTask->endFlag)) {
                delete restoreWriteTask;
                fileOperator->fdatasync();
                countdownLatch->countDown();
                gettimeofday(&t1, NULL);
                duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
                break;
            }
            chunkCounter++;

            if (restoreWriteTask->base) {
                gettimeofday(&rt1, NULL);
                pread(fd, deltaBuffer, restoreWriteTask->deltaLength, restoreWriteTask->pos);
                gettimeofday(&rt2, NULL);
                extraIO += restoreWriteTask->deltaLength;
                readTime += (rt2.tv_sec - rt1.tv_sec) * 1000000 + rt2.tv_usec - rt1.tv_usec;;
                gettimeofday(&dt1, NULL);

                int r = xd3_decode_memory(deltaBuffer, restoreWriteTask->deltaLength, restoreWriteTask->buffer,
                                          restoreWriteTask->length,
                                          oriBuffer, &oriSize, ChunkBufferSize, XD3_COMPLEVEL_1);
                gettimeofday(&dt2, NULL);
                decodingTime += (dt2.tv_sec - dt1.tv_sec) * 1000000 + dt2.tv_usec - dt1.tv_usec;
                assert(r == 0);
                pwrite(fd, oriBuffer, oriSize, restoreWriteTask->pos);
                normalIO += oriSize;
            } else {
                pwrite(fd, restoreWriteTask->buffer, restoreWriteTask->length, restoreWriteTask->pos);
                normalIO += restoreWriteTask->length;
            }

            syncCounter++;
            if(syncCounter > 1024){
                fileFlusher.addTask(1);
                syncCounter = 0;
            }

            delete restoreWriteTask;
            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec - t0.tv_usec;
        }
        free(deltaBuffer);
        free(oriBuffer);
    }


    CountdownLatch *countdownLatch;
    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<RestoreWriteTask *> taskList;
    MutexLock mutexLock;
    Condition condition;
    FileOperator *fileOperator = nullptr;

    uint64_t totalSize = 0;
    uint64_t deltaCounter = 0, chunkCounter = 0;

    uint64_t duration = 0;
    uint64_t decodingTime = 0;
    uint64_t readTime = 0;

    uint64_t syncCounter = 0;

    uint64_t extraIO = 0;
    uint64_t normalIO = 0;
};

static RestoreWritePipeline *GlobalRestoreWritePipelinePtr;

#endif //MEGA_RESTOREWRITEPIPELINE_H
