/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_CONTAINERCONSTRUCTOR_H
#define MEGA_CONTAINERCONSTRUCTOR_H

#include "Likely.h"
#include <zstd.h>
#include <atomic>

extern std::string ClassFilePath;
extern std::string VersionFilePath;
extern uint64_t ContainerSize;
uint64_t BufferCapacity = ContainerSize * 1.2;

struct timeval ct0, ct1;

struct WriteBuffer {
    uint64_t totalLength;
    uint64_t used;
    uint8_t *buffer = nullptr;
    uint8_t *compressBuffer = nullptr;

    void init() {
        buffer = (uint8_t *) malloc(BufferCapacity);
        compressBuffer = (uint8_t *) malloc(BufferCapacity);
        totalLength = ContainerSize;
        used = 0;
    }

    void write(uint8_t *buf, uint64_t len) {
        memcpy(buffer + used, buf, len);
        used += len;
    }

    void clear() {
        memset(buffer, 0, BufferCapacity);
        memset(compressBuffer, 0, BufferCapacity);
        used = 0;
    }

    void release() {
        free(buffer);
        free(compressBuffer);
    }
};

struct Container {
    Container(uint64_t s, uint64_t e, uint64_t c, uint8_t *b, uint64_t l) {
        lcs = s;
        lce = e;
        cid = c;
        buffer = b;
        length = l;
    }

    uint64_t lcs, lce, cid;
    uint8_t *buffer;
    uint64_t length;
    uint8_t *compressed;
    uint64_t compressedLength;
    bool written = false;
};

class OfflineReleaser {
public:
    OfflineReleaser() : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock) {
        worker = new std::thread(std::bind(&OfflineReleaser::ReleaserCallback, this));
    }

    int addTask(Container *con) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(con);
        taskAmount++;
    }

    int notify() {
        condition.notify();
    }

    int getContainer(uint64_t c, uint8_t *buffer, uint64_t *length) {
        MutexLockGuard mutexLockGuard(mutexLock);
        uint64_t last;
        for (auto item: taskList) {
            if (item->cid == c) {
                memcpy(buffer, item->buffer, item->length);
                *length = item->length;
                return 1;
            }
            last = item->cid;
        }
        return 0;
    }

    ~OfflineReleaser() {
        addTask(NULL);
        notify();
        worker->join();
    }

private:
    void ReleaserCallback() {
        pthread_setname_np(pthread_self(), "Releaser");
        Container *task;
        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                auto iter = taskList.begin();
                while (iter == taskList.end() || (*iter != NULL && (*iter)->written == false )) {
                    condition.wait();
                    iter = taskList.begin();
                }
                taskAmount--;
                task = taskList.front();
                taskList.pop_front();
            }

            if (task == NULL) {
                break;
            }

            free(task->buffer);
            free(task->compressed);
            delete task;
        }
    }

    std::thread *worker;
    bool runningFlag;
    uint64_t taskAmount;
    std::list<Container *> taskList;
    MutexLock mutexLock;
    Condition condition;
};

class OfflineWriter {
public:
    OfflineWriter(OfflineReleaser *offRls) : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock),
                                             offlineReleaser(offRls) {
        worker = new std::thread(std::bind(&OfflineWriter::fileFlusherCallback, this));
    }

    int addTask(Container *con) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(con);
        taskAmount++;
        condition.notify();
    }

    ~OfflineWriter() {
        addTask(NULL);
        worker->join();
    }

private:
    void fileFlusherCallback() {
        pthread_setname_np(pthread_self(), "Flusher");
        Container *task;
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

            if (task == NULL) {
                break;
            }

            sprintf(pathBuffer, ClassFilePath.data(), task->lcs, task->lce, task->cid);
            FileOperator *writer = new FileOperator(pathBuffer, FileOpenType::Write);
            writer->write(task->compressed, task->compressedLength);
            writer->fsync();
            writer->releaseBufferedData();
            delete writer;

            task->written = true;
            offlineReleaser->notify();

        }
    }

    std::thread *worker;
    char pathBuffer[256];
    bool runningFlag;
    uint64_t taskAmount;
    std::list<Container *> taskList;
    MutexLock mutexLock;
    Condition condition;
    OfflineReleaser *offlineReleaser;
};

class OfflineCompressor {
public:
    OfflineCompressor(OfflineReleaser *offlineReleaser) : runningFlag(true), taskAmount(0), mutexLock(),
                                                          condition(mutexLock),
                                                          offlineWriter(offlineReleaser) {
        sizeBeforeCompression = 0;
      sizeAfterCompression = 0;
      worker = new std::thread(std::bind(&OfflineCompressor::compressCallback, this));
    }

    int addTask(Container *con) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(con);
        taskAmount++;
        condition.notify();
    }

    ~OfflineCompressor() {
      printf("[ContainerConstructor] Compression Time : %lu\n", compressionTime);
//        printf("BeforeCompression:%lu, AfterCompression:%lu, CompressionReduce:%lu, CompressionRatio:%f\n",
//               (uint64_t) sizeBeforeCompression, (uint64_t) sizeAfterCompression,
//               sizeBeforeCompression - sizeAfterCompression,
//               (float) sizeBeforeCompression / sizeAfterCompression);
      GlobalMetadataManagerPtr->setAfterCompression(sizeAfterCompression);
      addTask(NULL);
      worker->join();
    }

private:
    void compressCallback() {
        pthread_setname_np(pthread_self(), "Cmp");
        Container *task;
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

            if (task == NULL) {
                break;
            }

            uint8_t *compressBuffer = (uint8_t *) malloc(BufferCapacity);
            gettimeofday(&ct0, NULL);
            size_t compressedSize = ZSTD_compress(compressBuffer, BufferCapacity, task->buffer,
                                                  task->length, 1);
            gettimeofday(&ct1, NULL);
            compressionTime += (ct1.tv_sec - ct0.tv_sec) * 1000000 + ct1.tv_usec - ct0.tv_usec;
            assert(!ZSTD_isError(compressedSize));

            sizeBeforeCompression += task->length;
            sizeAfterCompression += compressedSize;

            task->compressed = compressBuffer;
            task->compressedLength = compressedSize;

            offlineWriter.addTask(task);
        }
    }

    std::thread *worker;
    bool runningFlag;
    uint64_t taskAmount;
    std::list<Container *> taskList;
    MutexLock mutexLock;
    Condition condition;

    std::atomic<uint64_t> sizeBeforeCompression;
    std::atomic<uint64_t> sizeAfterCompression;

    uint64_t compressionTime = 0;

    OfflineWriter offlineWriter;
};

class ContainerConstructor {
public:
    ContainerConstructor(uint64_t cv) : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock),
                                        offlineCompressor(&offlineReleaser) {
        currentVersion = cv;

        writeBuffer.init();
    }

    int getContainer(uint64_t s, uint64_t e, uint64_t c, uint8_t *buffer, uint64_t *length) {
        if (s == currentVersion && e == currentVersion) {
            return offlineReleaser.getContainer(c, buffer, length);
        } else {
            return 0;
        }
    }

    int writeClass(uint8_t *header, uint64_t headerLen, uint8_t *buffer, uint64_t bufferLen) {
        writeBuffer.write(header, headerLen);
        writeBuffer.write(buffer, bufferLen);

        if (writeBuffer.used >= writeBuffer.totalLength) {
            flush();
            prepareNew();
        }
        return 0;
    }

    ~ContainerConstructor() {
        flush();
        writeBuffer.release();
    }

private:

    int flush() {
        Container *con = new Container(currentVersion, currentVersion, containerCounter,
                                       (uint8_t *) malloc(writeBuffer.used), writeBuffer.used);
        memcpy(con->buffer, writeBuffer.buffer, writeBuffer.used);
        offlineCompressor.addTask(con);
        offlineReleaser.addTask(con);
    }

    int prepareNew() {
        containerCounter++;
        writeBuffer.clear();
    }

    WriteBuffer writeBuffer;
    uint64_t currentVersion;

    uint64_t containerCounter = 0;

    bool runningFlag;
    uint64_t taskAmount;
    std::list<uint64_t> taskList;
    MutexLock mutexLock;
    Condition condition;

    OfflineReleaser offlineReleaser;
    OfflineCompressor offlineCompressor;

};

#endif //MEGA_CONTAINERCONSTRUCTOR_H
