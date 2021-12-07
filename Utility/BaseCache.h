/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_BASECACHE_H
#define MEGA_BASECACHE_H

#include <unordered_map>
#include <map>

extern std::string ClassFileAppendPath;
extern uint64_t ContainerSize;
uint64_t PreloadSize = ContainerSize * 1.2;

DEFINE_uint64(CacheSize,
              128, "CappingThreshold");

uint64_t TotalSizeThreshold = FLAGS_CacheSize * 4 * 1024 * 1024;

int UpdateScore = 2;

class BaseCache {
public:
    BaseCache() : totalSize(0), index(0), cacheMap(65536), write(0), read(0) {
        preloadBuffer = (uint8_t *) malloc(PreloadSize);
        decompressBuffer = (uint8_t *) malloc(PreloadSize);
    }

    void setCurrentVersion(uint64_t version) {
        currentVersion = version;
    }

    ~BaseCache() {
        statistics();
        free(preloadBuffer);
        free(decompressBuffer);
        for (const auto &blockEntry: cacheMap) {
            free(blockEntry.second.block);
        }
    }

    void statistics(){
        printf("block cache:\n");
        printf("total size:%lu, items:%lu\n", totalSize, items);
        printf("cache write:%lu, cache read:%lu\n", write, read);
        printf("hit rate: %f(%lu/%lu)\n", float(success)/access, success, access);
        printf("cache miss %lu times, total loading time %lu us, average %f us\n", access - success, loadingTime,
               (float) loadingTime / (access - success));
        printf("self hit:%lu\n", selfHit);
        printf("cache prefetching size:%lu\n", prefetching);
    }

    void loadBaseChunks(const BasePos& basePos) {
        gettimeofday(&t0, NULL);
        char pathBuffer[256];

        if (basePos.CategoryOrder == currentVersion) {
            sprintf(pathBuffer, ClassFilePath.data(), basePos.CategoryOrder, currentVersion, basePos.cid);
            selfHit++;
        } else if (basePos.CategoryOrder) {
            sprintf(pathBuffer, ClassFilePath.data(), basePos.CategoryOrder, currentVersion - 1, basePos.cid);
        } else {
            sprintf(pathBuffer, ClassFileAppendPath.data(), 1, currentVersion - 1, basePos.cid);
        }

        uint64_t readSize = 0;
        {
            FileOperator basefile(pathBuffer, FileOpenType::Read);
            uint64_t decompressSize = basefile.read(decompressBuffer, PreloadSize);
            basefile.releaseBufferedData();
            prefetching += decompressSize;

            readSize = ZSTD_decompress(preloadBuffer, PreloadSize, decompressBuffer, decompressSize);
            assert(!ZSTD_isError(readSize));

            assert(basePos.length <= readSize);
        }

        BlockHeader *headPtr;

        uint64_t preLoadPos = 0;
        uint64_t leftLength = readSize;

        while (leftLength > sizeof(BlockHeader) &&
               leftLength >= (2048 + sizeof(BlockHeader))) {// todo: min chunksize configured to 2048
            headPtr = (BlockHeader *) (preloadBuffer + preLoadPos);
            if (headPtr->length + sizeof(BlockHeader) > leftLength) {
                break;
            } else if (!headPtr->type) {
                addRecord(headPtr->fp, preloadBuffer + preLoadPos + sizeof(BlockHeader),
                          headPtr->length);
            }

            preLoadPos += headPtr->length + sizeof(BlockHeader);
            if (preLoadPos >= readSize) break;
            leftLength = readSize - preLoadPos;
        }
        gettimeofday(&t1, NULL);
        loadingTime += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
    }

    void addRecord(const SHA1FP &sha1Fp, uint8_t *buffer, uint64_t length) {
        {
            //MutexLockGuard cacheLockGuard(cacheLock);
            auto iter = cacheMap.find(sha1Fp);
            if (iter == cacheMap.end()) {
                uint8_t *cacheBuffer = (uint8_t *) malloc(length);
                memcpy(cacheBuffer, buffer, length);
                cacheMap[sha1Fp] = {
                        cacheBuffer, length, index,
                };
                items++;
                write += length;
                {
                    //MutexLockGuard lruLockGuard(lruLock);
                    lruList[index] = sha1Fp;
                    index++;
                    totalSize += length;
                    while (totalSize > TotalSizeThreshold) {
                        auto iterLru = lruList.begin();
                        assert(iterLru != lruList.end());
                        auto iterCache = cacheMap.find(iterLru->second);
                        assert(iterCache != cacheMap.end());
                        totalSize -= iterCache->second.length;
                        free(iterCache->second.block);
                        cacheMap.erase(iterCache);
                        lruList.erase(iterLru);
                        items--;
                    }
                }
            } else {
                // it should not happen
                freshLastVisit(iter);
            }
        }
    }

    int getRecordBatch(BasePos *chunks, int count, BlockEntry *cacheBlock, BasePos *selectedBase) {
        {
            //MutexLockGuard cacheLockGuard(cacheLock);
            access++;
            int vadID = -1;
            for (int i = 0; i < 6; i++) {
                if (chunks[i].valid) {
                    *selectedBase = chunks[i];
                    vadID = i;
                    auto iterCache = cacheMap.find(chunks[i].sha1Fp);
                    if (iterCache == cacheMap.end()) {
                        //
                    } else {
                        success++;
                        *cacheBlock = iterCache->second;
                        read += cacheBlock->length;
                        {
                            freshLastVisit(iterCache);
                        }
                        return 1;
                    }
                }
            }
            loadBaseChunks(chunks[vadID]);
            auto iterCache = cacheMap.find(chunks[vadID].sha1Fp);
            if (iterCache == cacheMap.end()) {
                printf("id:%d, co:%u\n", vadID, chunks[vadID].CategoryOrder);
            }
            assert(iterCache != cacheMap.end());
            *cacheBlock = iterCache->second;
            {
                freshLastVisit(iterCache);
            }
            return 1;
        }
    }

    int getRecord(const BasePos *basePos, BlockEntry *cacheBlock) {
        {
            //MutexLockGuard cacheLockGuard(cacheLock);
            auto iterCache = cacheMap.find(basePos->sha1Fp);
            if (iterCache != cacheMap.end()) {
                *cacheBlock = iterCache->second;
                read += cacheBlock->length;
                {
                    freshLastVisit(iterCache);
                }
                return 1;
            }
            return 0;
        }
    }

    int getRecordWithoutFresh(const BasePos *basePos, BlockEntry *cacheBlock) {
        {
            //MutexLockGuard cacheLockGuard(cacheLock);
            access++;
            auto iterCache = cacheMap.find(basePos->sha1Fp);
            if (iterCache != cacheMap.end()) {
                success++;
                *cacheBlock = iterCache->second;
                read += cacheBlock->length;
                return 1;
            }
            return 0;
        }
    }

    int getRecordNoFS(const BasePos *basePos, BlockEntry *cacheBlock) {
        {
            //MutexLockGuard cacheLockGuard(cacheLock);
            auto iterCache = cacheMap.find(basePos->sha1Fp);
            if (iterCache != cacheMap.end()) {
                *cacheBlock = iterCache->second;
                read += cacheBlock->length;
                return 1;
            }
            return 0;
        }
    }

private:
    void freshLastVisit(
            std::unordered_map<SHA1FP, BlockEntry, TupleHasher, TupleEqualer>::iterator iter) {
        //MutexLockGuard lruLockGuard(lruLock);
        iter->second.score++;
        if (iter->second.score > UpdateScore) {
            iter->second.score = 0;
            auto iterl = lruList.find(iter->second.lastVisit);
            lruList[index] = iterl->second;
            lruList.erase(iterl);
            iter->second.lastVisit = index;
            index++;
        }

    }

    struct timeval t0, t1;
    uint64_t index;
    uint64_t totalSize;
    std::unordered_map<SHA1FP, BlockEntry, TupleHasher, TupleEqualer> cacheMap;
    std::map<uint64_t, SHA1FP> lruList;
    //MutexLock cacheLock;
    //MutexLock lruLock;
    uint64_t write, read;
    uint64_t access = 0, success = 0;
    uint64_t loadingTime = 0;
    uint64_t items = 0;
    uint64_t currentVersion = 0;
    uint64_t selfHit = 0;

    uint8_t *preloadBuffer = nullptr;
    uint8_t *decompressBuffer = nullptr;

    uint64_t prefetching = 0;
};

#endif //MEGA_BASECACHE_H
