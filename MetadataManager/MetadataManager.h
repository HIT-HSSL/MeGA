/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_MATADATAMANAGER_H
#define MEGA_MATADATAMANAGER_H

#include <map>
#include "../Utility/StorageTask.h"
#include <unordered_set>
#include <unordered_map>
#include "../Utility/md5.h"
#include "../Utility/xxhash.h"
#include <random>

#define SeedLength 64
#define SymbolTypes 256
#define MD5Length 16

uint64_t shadMask = 0x7;

int ReplaceThreshold = 10;

extern uint64_t TotalVersion;
extern std::string KVPath;

struct TupleHasher {
    std::size_t
    operator()(const SHA1FP &key) const {
        return key.fp1;
    }
};

struct TupleEqualer {
    bool operator()(const SHA1FP &lhs, const SHA1FP &rhs) const {
        return lhs.fp1 == rhs.fp1 && lhs.fp2 == rhs.fp2 && lhs.fp3 == rhs.fp3 && lhs.fp4 == rhs.fp4;
    }
};

struct FPTableEntry {
    uint32_t deltaTag: 1; // 0: unique 1: delta
    uint32_t categoryOrder: 31;
    union {
        uint64_t oriLength;
    };
    union {
        SHA1FP baseFP;
    };
};

enum class LookupResult {
    Unique,
    InternalDedup,
    AdjacentDedup,
    InternalDeltaDedup, // reference to a delta chunk
    Similar,
    Dissimilar,
};

struct FPIndex{
    uint64_t duplicateSize = 0;
    uint64_t totalSize = 0;
    std::unordered_map<SHA1FP, FPTableEntry, TupleHasher, TupleEqualer> fpTable;

    void rolling(FPIndex& alter){
        fpTable.clear();
        fpTable.swap(alter.fpTable);
        duplicateSize = alter.duplicateSize;
        totalSize = alter.totalSize;
        alter.duplicateSize = 0;
        alter.totalSize = 0;
    }
};

struct SimilarityIndex{
    std::unordered_map<uint64_t, BasePos> simIndex1;
    std::unordered_map<uint64_t, BasePos> simIndex2;
    std::unordered_map<uint64_t, BasePos> simIndex3;

    void rolling(SimilarityIndex& alter){
        simIndex1.clear();
        simIndex2.clear();
        simIndex3.clear();
        simIndex1.swap(alter.simIndex1);
        simIndex2.swap(alter.simIndex2);
        simIndex3.swap(alter.simIndex3);
    }
};

uint64_t* gearMatrix;
int* kArray;
int* bArray;
uint64_t* maxList;

void odessInit(){
    gearMatrix = (uint64_t*)malloc(sizeof(uint64_t)*SymbolTypes);
    kArray = (int*)malloc(sizeof(int)*12);
    bArray = (int*)malloc(sizeof(int)*12);
    maxList = (uint64_t*)malloc(sizeof(uint64_t) * 12);
    char seed[SeedLength];
    for (int i = 0; i < SymbolTypes; i++) {
        for (int j = 0; j < SeedLength; j++) {
            seed[j] = i;
        }

        gearMatrix[i] = 0;
        char md5_result[MD5Length];
        md5_state_t md5_state;
        md5_init(&md5_state);
        md5_append(&md5_state, (md5_byte_t *) seed, SeedLength);
        md5_finish(&md5_state, (md5_byte_t *) md5_result);

        memcpy(&gearMatrix[i], md5_result, sizeof(uint64_t));
    }

    {
        std::default_random_engine randomEngine;
        std::uniform_int_distribution<uint64_t> distributionA;
        std::uniform_int_distribution<uint64_t> distributionB;

        std::uniform_int_distribution<uint64_t>::param_type paramA(0x0000000000100000, 0x0000000010000000);
        std::uniform_int_distribution<uint64_t>::param_type paramB(0x0000000000100000, 0x00000000ffffffff);
        distributionA.param(paramA);
        distributionB.param(paramB);
        for (int i = 0; i < 12; i++) {
            kArray[i] = distributionA(randomEngine);
            bArray[i] = distributionB(randomEngine);
            maxList[i] = 0; // min uint64_t
        }
    }
}

void odessDeinit(){
    free(gearMatrix);
    free(kArray);
    free(bArray);
    free(maxList);
}

void odessCalculation(uint8_t* buffer, uint64_t length, SimilarityFeatures* similarityFeatures){
    uint64_t hashValue = 0;
    for (uint64_t i = 0; i < length; i++) {
        hashValue = (hashValue << 1) + gearMatrix[buffer[i]];
        if (!(hashValue & 0x0000400303410000)) { // sampling mask
            for (int j = 0; j < 12; j++) {
                uint64_t transResult = (hashValue * kArray[j] + bArray[j]);
                if (transResult > maxList[j])
                    maxList[j] = transResult;
            }
        }
    }

    similarityFeatures->feature1 = XXH64(&maxList[0 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);
    similarityFeatures->feature2 = XXH64(&maxList[1 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);
    similarityFeatures->feature3 = XXH64(&maxList[2 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);
    memset(maxList, 0, sizeof(uint64_t)*12);
}

class MetadataManager {
public:
    MetadataManager() {
        odessInit();
    }

    ~MetadataManager(){
        odessDeinit();
    }

    LookupResult dedupLookup(const SHA1FP &sha1Fp, uint64_t chunkSize, FPTableEntry* fpTableEntry) {
        MutexLockGuard mutexLockGuard(tableLock);
        auto innerDedupIter = laterTable.fpTable.find(sha1Fp);
        if (innerDedupIter != laterTable.fpTable.end()) {
            if(innerDedupIter->second.deltaTag){
                *fpTableEntry = innerDedupIter->second;
                return LookupResult::InternalDeltaDedup;
            }else{
                return LookupResult::InternalDedup;
            }
        }

        laterTable.totalSize += chunkSize + sizeof(BlockHeader);
        auto neighborDedupIter = earlierTable.fpTable.find(sha1Fp);
        if (neighborDedupIter == earlierTable.fpTable.end()) {
            return LookupResult::Unique;
        } else {
            laterTable.duplicateSize += chunkSize + sizeof(BlockHeader);
            *fpTableEntry = neighborDedupIter->second;
            return LookupResult::AdjacentDedup;
        }
    }

    LookupResult similarityLookup(const SimilarityFeatures& similarityFeatures, BasePos* basePos){
        memset(basePos, 0, sizeof(BasePos) * 3);
        bool result = false;
        auto iter1 = earlierSimilarityTable.simIndex1.find(similarityFeatures.feature1);
        if (iter1 != earlierSimilarityTable.simIndex1.end()) {
            basePos[0] = iter1->second;
            basePos[0].valid = 1;
            result = true;
        }
        auto iter2 = earlierSimilarityTable.simIndex2.find(similarityFeatures.feature2);
        if (iter2 != earlierSimilarityTable.simIndex2.end()) {
            basePos[1] = iter2->second;
            basePos[1].valid = 1;
            result = true;
        }
        auto iter3 = earlierSimilarityTable.simIndex3.find(similarityFeatures.feature3);
        if (iter3 != earlierSimilarityTable.simIndex3.end()) {
            basePos[2] = iter3->second;
            basePos[2].valid = 1;
            result = true;
        }
        if (result) {
            return LookupResult::Similar;
        } else {
            return LookupResult::Dissimilar;
        }
    }

    uint64_t arrangementGetTruncateSize(){
        return earlierTable.totalSize - laterTable.duplicateSize;
    }

    int arrangementLookup(const SHA1FP &sha1Fp) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto r = laterTable.fpTable.find(sha1Fp);
        if (r == laterTable.fpTable.end()) {
            return 0;
        } else {
            return 1;
        }
    }

    int uniqueAddRecord(const SHA1FP &sha1Fp, uint32_t categoryOrder, uint64_t oriLength) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto pp = laterTable.fpTable.find(sha1Fp);
        assert(pp == laterTable.fpTable.end());

        laterTable.fpTable.insert({sha1Fp, {0, categoryOrder, oriLength}});

        return 0;
    }

    int deltaAddRecord(const SHA1FP &sha1Fp, uint32_t categoryOrder, const SHA1FP &baseFP, uint64_t diffLength,
                       uint64_t oriLength) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto pp = laterTable.fpTable.find(sha1Fp);
        assert(pp == laterTable.fpTable.end());

        laterTable.fpTable.insert({sha1Fp, {1, categoryOrder, oriLength, baseFP}});
        laterTable.duplicateSize -= diffLength;

        return 0;
    }

    int addSimilarFeature(const SimilarityFeatures &similarityFeatures, const BasePos &basePos){
        laterSimilarityTable.simIndex1.emplace(similarityFeatures.feature1, basePos);
        laterSimilarityTable.simIndex2.emplace(similarityFeatures.feature2, basePos);
        laterSimilarityTable.simIndex3.emplace(similarityFeatures.feature3, basePos);
        return 0;
    }

    int neighborAddRecord(const SHA1FP &sha1Fp, const FPTableEntry& fpTableEntry) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto pp = laterTable.fpTable.find(sha1Fp);
        //assert(pp == laterTable.fpTable.end());

        laterTable.fpTable.insert({sha1Fp, fpTableEntry});
        return 0;
    }

    int extendBase(const SHA1FP &sha1Fp, const FPTableEntry& fpTableEntry) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto pp = laterTable.fpTable.find(sha1Fp);
        if(pp == laterTable.fpTable.end()) {
            laterTable.fpTable.insert({sha1Fp, fpTableEntry});
            laterTable.duplicateSize += fpTableEntry.oriLength + sizeof(BlockHeader); // updated
        }

        return 0;
    }

    int tableRolling() {
        MutexLockGuard mutexLockGuard(tableLock);

        earlierTable.rolling(laterTable);
        earlierSimilarityTable.rolling(laterSimilarityTable);

        return 0;
    }

    int similarityTableMerge(){
        for(auto& item: earlierSimilarityTable.simIndex1){
            if(item.second.CategoryOrder >= 3){
                item.second.CategoryOrder--;
            }else if(item.second.CategoryOrder == 2){
                item.second.CategoryOrder = 0;
            }
        }
        for(auto& item: earlierSimilarityTable.simIndex2){
            if(item.second.CategoryOrder >= 3){
                item.second.CategoryOrder--;
            }else if(item.second.CategoryOrder == 2){
                item.second.CategoryOrder = 0;
            }
        }
        for(auto& item: earlierSimilarityTable.simIndex3){
            if(item.second.CategoryOrder >= 3){
                item.second.CategoryOrder--;
            }else if(item.second.CategoryOrder == 2){
                item.second.CategoryOrder = 0;
            }
        }
        return 0;
    }

    // updated
    int save(){
        printf("------------------------Saving index----------------------\n");
        printf("Saving index..\n");
        uint64_t size;
        FileOperator fileOperator((char*)KVPath.data(), FileOpenType::Write);

        fileOperator.write((uint8_t*)&earlierTable, sizeof(uint64_t)*2);
        size = earlierTable.fpTable.size();
        fileOperator.write((uint8_t*)&size, sizeof(uint64_t));
        for(auto item : earlierTable.fpTable){
            fileOperator.write((uint8_t*)&item.first, sizeof(SHA1FP));
            fileOperator.write((uint8_t*)&item.second, sizeof(FPTableEntry));
        }
        printf("earlier table saves %lu items\n", size);
        printf("earlier total size:%lu, duplicate size:%lu\n", earlierTable.totalSize, earlierTable.duplicateSize);
        size = earlierSimilarityTable.simIndex1.size();
        fileOperator.write((uint8_t*)&size, sizeof(uint64_t));
        for(auto item : earlierSimilarityTable.simIndex1){
            fileOperator.write((uint8_t*)&item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t*)&item.second, sizeof(BasePos));
        }
        printf("earlier similar table1 saves %lu items\n", size);
        size = earlierSimilarityTable.simIndex2.size();
        fileOperator.write((uint8_t*)&size, sizeof(uint64_t));
        for(auto item : earlierSimilarityTable.simIndex2){
            fileOperator.write((uint8_t*)&item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t*)&item.second, sizeof(BasePos));
        }
        printf("earlier similar table2 saves %lu items\n", size);
        size = earlierSimilarityTable.simIndex3.size();
        fileOperator.write((uint8_t*)&size, sizeof(uint64_t));
        for(auto item : earlierSimilarityTable.simIndex3){
            fileOperator.write((uint8_t*)&item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t*)&item.second, sizeof(BasePos));
        }
        printf("earlier similar table3 saves %lu items\n", size);

        fileOperator.write((uint8_t*)&laterTable, sizeof(uint64_t)*2);
        size = laterTable.fpTable.size();
        fileOperator.write((uint8_t*)&size, sizeof(uint64_t));
        for(auto item : laterTable.fpTable){
            fileOperator.write((uint8_t*)&item.first, sizeof(SHA1FP));
            fileOperator.write((uint8_t*)&item.second, sizeof(FPTableEntry));
        }
        printf("later table saves %lu items\n", size);
        printf("later total size:%lu, duplicate size:%lu\n", laterTable.totalSize, laterTable.duplicateSize);
        size = laterSimilarityTable.simIndex1.size();
        fileOperator.write((uint8_t*)&size, sizeof(uint64_t));
        for(auto item : laterSimilarityTable.simIndex1){
            fileOperator.write((uint8_t*)&item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t*)&item.second, sizeof(BasePos));
        }
        printf("later similar table1 saves %lu items\n", size);
        size = laterSimilarityTable.simIndex2.size();
        fileOperator.write((uint8_t*)&size, sizeof(uint64_t));
        for(auto item : laterSimilarityTable.simIndex2){
            fileOperator.write((uint8_t*)&item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t*)&item.second, sizeof(BasePos));
        }
        printf("later similar table2 saves %lu items\n", size);
        size = laterSimilarityTable.simIndex3.size();
        fileOperator.write((uint8_t *) &size, sizeof(uint64_t));
        for (auto item : laterSimilarityTable.simIndex3) {
            fileOperator.write((uint8_t *) &item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t *) &item.second, sizeof(BasePos));
        }
        printf("later similar table3 saves %lu items\n", size);

        fileOperator.fdatasync();

        return 0;
    }

    int load(){
        printf("-----------------------Loading index-----------------------\n");
        printf("Loading index..\n");
        uint64_t sizeE = 0;
        uint64_t sizeL = 0;
        SHA1FP tempFP;
        FPTableEntry tempFPTableEntry;
        uint64_t tempFeature;
        BasePos tempBasePos;
        FileOperator fileOperator((char*)KVPath.data(), FileOpenType::Read);
        assert(earlierTable.fpTable.size() == 0);
        assert(laterTable.fpTable.size() == 0);

        fileOperator.read((uint8_t*)&earlierTable, sizeof(uint64_t)*2);
        fileOperator.read((uint8_t*)&sizeE, sizeof(uint64_t));
        for(uint64_t i = 0; i<sizeE; i++){
            fileOperator.read((uint8_t*)&tempFP, sizeof(SHA1FP));
            fileOperator.read((uint8_t*)&tempFPTableEntry, sizeof(FPTableEntry));
            earlierTable.fpTable.insert({tempFP, tempFPTableEntry});
        }
        printf("earlier table load %lu items\n", sizeE);
        fileOperator.read((uint8_t*)&sizeE, sizeof(uint64_t));
        for(uint64_t i = 0; i<sizeE; i++){
            fileOperator.read((uint8_t*)&tempFeature, sizeof(uint64_t));
            fileOperator.read((uint8_t*)&tempBasePos, sizeof(BasePos));
            earlierSimilarityTable.simIndex1.insert({tempFeature, tempBasePos});
        }
        printf("earlier similar table1 load %lu items\n", sizeE);
        fileOperator.read((uint8_t*)&sizeE, sizeof(uint64_t));
        for(uint64_t i = 0; i<sizeE; i++){
            fileOperator.read((uint8_t*)&tempFeature, sizeof(uint64_t));
            fileOperator.read((uint8_t*)&tempBasePos, sizeof(BasePos));
            earlierSimilarityTable.simIndex2.insert({tempFeature, tempBasePos});
        }
        printf("earlier similar table2 load %lu items\n", sizeE);
        fileOperator.read((uint8_t*)&sizeE, sizeof(uint64_t));
        for(uint64_t i = 0; i<sizeE; i++){
            fileOperator.read((uint8_t*)&tempFeature, sizeof(uint64_t));
            fileOperator.read((uint8_t*)&tempBasePos, sizeof(BasePos));
            earlierSimilarityTable.simIndex3.insert({tempFeature, tempBasePos});
        }
        printf("earlier similar table3 load %lu items\n", sizeE);


        fileOperator.read((uint8_t*)&earlierTable, sizeof(uint64_t)*2);
        fileOperator.read((uint8_t*)&sizeL, sizeof(uint64_t));
        for(uint64_t i = 0; i<sizeL; i++){
            fileOperator.read((uint8_t*)&tempFP, sizeof(SHA1FP));
            fileOperator.read((uint8_t*)&tempFPTableEntry, sizeof(FPTableEntry));
            laterTable.fpTable.insert({tempFP, tempFPTableEntry});
        }
        printf("later table load %lu items\n", sizeL);
        fileOperator.read((uint8_t*)&sizeL, sizeof(uint64_t));
        for(uint64_t i = 0; i<sizeL; i++){
            fileOperator.read((uint8_t*)&tempFeature, sizeof(uint64_t));
            fileOperator.read((uint8_t*)&tempBasePos, sizeof(BasePos));
            laterSimilarityTable.simIndex1.insert({tempFeature, tempBasePos});
        }
        printf("later similar table1 load %lu items\n", sizeL);
        fileOperator.read((uint8_t*)&sizeL, sizeof(uint64_t));
        for(uint64_t i = 0; i<sizeL; i++){
            fileOperator.read((uint8_t*)&tempFeature, sizeof(uint64_t));
            fileOperator.read((uint8_t*)&tempBasePos, sizeof(BasePos));
            laterSimilarityTable.simIndex2.insert({tempFeature, tempBasePos});
        }
        printf("later similar table2 load %lu items\n", sizeL);
        fileOperator.read((uint8_t *) &sizeL, sizeof(uint64_t));
        for (uint64_t i = 0; i < sizeL; i++) {
            fileOperator.read((uint8_t *) &tempFeature, sizeof(uint64_t));
            fileOperator.read((uint8_t *) &tempBasePos, sizeof(BasePos));
            laterSimilarityTable.simIndex3.insert({tempFeature, tempBasePos});
        }
        printf("later similar table3 load %lu items\n", sizeL);

        return 0;
    }

private:
    FPIndex earlierTable;
    FPIndex laterTable;
    SimilarityIndex earlierSimilarityTable;
    SimilarityIndex laterSimilarityTable;

    MutexLock tableLock;
};

static MetadataManager *GlobalMetadataManagerPtr;

#endif //MEGA_MATADATAMANAGER_H