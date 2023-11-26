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
    uint64_t oriLength;
    uint64_t length;
    SHA1FP baseFP;
};

struct FPIndex {
    uint64_t migrateSize = 0;
    uint64_t totalSize = 0;
    std::unordered_map<SHA1FP, FPTableEntry, TupleHasher, TupleEqualer> fpTable;

    void rolling(FPIndex &alter) {
        fpTable.clear();
        fpTable.swap(alter.fpTable);
        migrateSize = alter.migrateSize;
        totalSize = alter.totalSize;
        alter.migrateSize = 0;
        alter.totalSize = 0;
    }
};

struct SimilarityIndex {
    std::unordered_map<uint64_t, BasePos> simIndex1;
    std::unordered_map<uint64_t, BasePos> simIndex2;
    std::unordered_map<uint64_t, BasePos> simIndex3;

    void rolling(SimilarityIndex &alter) {
        simIndex1.clear();
        simIndex2.clear();
        simIndex3.clear();
        simIndex1.swap(alter.simIndex1);
        simIndex2.swap(alter.simIndex2);
        simIndex3.swap(alter.simIndex3);
    }
};

uint64_t *gearMatrix;
int kArray[12] = {
        1007,
        459326,
        48011,
        935033,
        831379,
        530326,
        384145,
        687288,
        846569,
        654457,
        910678,
        48431
};
int bArray[12] = {
        1623698648,
        -1676223803,
        -687657152,
        1116486740,
        115856562,
        -2129903346,
        897592878,
        -148337918,
        -1948941976,
        1506843910,
        -1582821563,
        1441557442,
};
uint64_t *maxList;

void odessInit() {
    gearMatrix = (uint64_t *) malloc(sizeof(uint64_t) * SymbolTypes);
//    kArray = (int*)malloc(sizeof(int)*12);
//    bArray = (int*)malloc(sizeof(int)*12);
    maxList = (uint64_t *) malloc(sizeof(uint64_t) * 12);
    memset(maxList, 0, sizeof(uint64_t) * 12);
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

//    {
//        std::default_random_engine randomEngine;
//        std::uniform_int_distribution<uint64_t> distributionA;
//        std::uniform_int_distribution<uint64_t> distributionB;
//
//        std::uniform_int_distribution<uint64_t>::param_type paramA(0x0000000000001000, 0x0000000001000000);
//        std::uniform_int_distribution<uint64_t>::param_type paramB(0x0000000000100000, 0x00000000ffffffff);
//        distributionA.param(paramA);
//        distributionB.param(paramB);
//        for (int i = 0; i < 12; i++) {
//            kArray[i] = distributionA(randomEngine);
//            bArray[i] = distributionB(randomEngine);
//            maxList[i] = 0; // min uint64_t
//        }
//    }
}

void odessDeinit() {
    free(gearMatrix);
    free(maxList);
}

void odessCalculation(uint8_t *buffer, uint64_t length, SimilarityFeatures *similarityFeatures) {
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
    memset(maxList, 0, sizeof(uint64_t) * 12);
}

class MetadataManager {
public:
    MetadataManager() {
        odessInit();
    }

    ~MetadataManager() {
        odessDeinit();
    }

    LookupResult dedupLookup(const SHA1FP &sha1Fp, uint64_t chunkSize, FPTableEntry *fpTableEntry) {
        MutexLockGuard mutexLockGuard(tableLock);
        auto innerDedupIter = laterTable.fpTable.find(sha1Fp);
        if (innerDedupIter != laterTable.fpTable.end()) {
            if (innerDedupIter->second.deltaTag) {
                *fpTableEntry = innerDedupIter->second;
                return LookupResult::InternalDeltaDedup;
            } else {
                return LookupResult::InternalDedup;
            }
        }

        laterTable.totalSize += chunkSize + sizeof(BlockHeader);
        auto neighborDedupIter = earlierTable.fpTable.find(sha1Fp);
        if (neighborDedupIter == earlierTable.fpTable.end()) {
            return LookupResult::Unique;
        } else {
            laterTable.migrateSize += chunkSize + sizeof(BlockHeader);
            *fpTableEntry = neighborDedupIter->second;
            return LookupResult::AdjacentDedup;
        }
    }

    int getBasePos(const SHA1FP &sha1Fp, uint64_t chunkSize, FPTableEntry *fpTableEntry) {
        MutexLockGuard mutexLockGuard(tableLock);
        auto innerDedupIter = laterTable.fpTable.find(sha1Fp);
        if (innerDedupIter != laterTable.fpTable.end()) {
            assert(!innerDedupIter->second.deltaTag);
            return 1;
        }

        auto neighborDedupIter = earlierTable.fpTable.find(sha1Fp);
        if (neighborDedupIter != earlierTable.fpTable.end()) {
            *fpTableEntry = neighborDedupIter->second;
            return 1;
        }
        return 0;
    }


    LookupResult similarityLookupSimple(const SimilarityFeatures &similarityFeatures, BasePos *basePos) {
        auto iter1 = earlierSimilarityTable.simIndex1.find(similarityFeatures.feature1);
        if (iter1 != earlierSimilarityTable.simIndex1.end()) {
            *basePos = iter1->second;
            return LookupResult::Similar;
        }
        auto iter2 = earlierSimilarityTable.simIndex2.find(similarityFeatures.feature2);
        if (iter2 != earlierSimilarityTable.simIndex2.end()) {
            *basePos = iter2->second;
            return LookupResult::Similar;
        }
        auto iter3 = earlierSimilarityTable.simIndex3.find(similarityFeatures.feature3);
        if (iter3 != earlierSimilarityTable.simIndex3.end()) {
            *basePos = iter3->second;
            return LookupResult::Similar;
        }
        auto iter4 = laterSimilarityTable.simIndex1.find(similarityFeatures.feature1);
        if (iter4 != laterSimilarityTable.simIndex1.end()) {
            *basePos = iter4->second;
            return LookupResult::Similar;
        }
        auto iter5 = laterSimilarityTable.simIndex2.find(similarityFeatures.feature2);
        if (iter5 != laterSimilarityTable.simIndex2.end()) {
            *basePos = iter5->second;
            return LookupResult::Similar;
        }
        auto iter6 = laterSimilarityTable.simIndex3.find(similarityFeatures.feature3);
        if (iter6 != laterSimilarityTable.simIndex3.end()) {
            *basePos = iter6->second;
            return LookupResult::Similar;
        }
        return LookupResult::Dissimilar;
    }

    LookupResult similarityLookup(const SimilarityFeatures &similarityFeatures, BasePos *basePos) {
        memset(basePos, 0, sizeof(BasePos) * 6);
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
        auto iter4 = laterSimilarityTable.simIndex1.find(similarityFeatures.feature1);
        if (iter4 != laterSimilarityTable.simIndex1.end()) {
            basePos[3] = iter4->second;
            basePos[3].valid = 1;
            result = true;
        }
        auto iter5 = laterSimilarityTable.simIndex2.find(similarityFeatures.feature2);
        if (iter5 != laterSimilarityTable.simIndex2.end()) {
            basePos[4] = iter5->second;
            basePos[4].valid = 1;
            result = true;
        }
        auto iter6 = laterSimilarityTable.simIndex3.find(similarityFeatures.feature3);
        if (iter6 != laterSimilarityTable.simIndex3.end()) {
            basePos[5] = iter6->second;
            basePos[5].valid = 1;
            result = true;
        }
        if (result) {
            return LookupResult::Similar;
        } else {
            return LookupResult::Dissimilar;
        }
    }

    uint64_t arrangementGetTruncateSize() {
        return earlierTable.totalSize - laterTable.migrateSize;
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

        laterTable.fpTable.insert({sha1Fp, {0, categoryOrder, oriLength, oriLength}});

        return 0;
    }

    int deltaAddRecord(const SHA1FP &sha1Fp, uint32_t categoryOrder, const SHA1FP &baseFP, uint64_t diffLength,
                       uint64_t oriLength) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto pp = laterTable.fpTable.find(sha1Fp);
        assert(pp == laterTable.fpTable.end());

        laterTable.fpTable.insert({sha1Fp, {1, categoryOrder, oriLength, oriLength - diffLength, baseFP}});
        laterTable.totalSize -= diffLength;

        return 0;
    }

    int addSimilarFeature(const SimilarityFeatures &similarityFeatures, const BasePos &basePos) {
        laterSimilarityTable.simIndex1.emplace(similarityFeatures.feature1, basePos);
        laterSimilarityTable.simIndex2.emplace(similarityFeatures.feature2, basePos);
        laterSimilarityTable.simIndex3.emplace(similarityFeatures.feature3, basePos);
        return 0;
    }

    int neighborAddRecord(const SHA1FP &sha1Fp, const FPTableEntry &fpTableEntry) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto pp = laterTable.fpTable.find(sha1Fp);
        //assert(pp == laterTable.fpTable.end());

        laterTable.fpTable.insert({sha1Fp, fpTableEntry});
        return 0;
    }

    int extendBase(const SHA1FP &sha1Fp, const FPTableEntry &fpTableEntry) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto pp = laterTable.fpTable.find(sha1Fp);
        if (pp == laterTable.fpTable.end()) {
            laterTable.fpTable.insert({sha1Fp, fpTableEntry});
            laterTable.migrateSize += fpTableEntry.oriLength + sizeof(BlockHeader); // updated
        }

        return 0;
    }

    int tableRolling() {
        MutexLockGuard mutexLockGuard(tableLock);

        earlierTable.rolling(laterTable);
        earlierSimilarityTable.rolling(laterSimilarityTable);

        return 0;
    }

    int similarityTableMerge() {
        for (auto &item: earlierSimilarityTable.simIndex1) {
            if (item.second.CategoryOrder >= 3) {
                item.second.CategoryOrder--;
            } else if (item.second.CategoryOrder == 2) {
                item.second.CategoryOrder = 0;
            }
        }
        for (auto &item: earlierSimilarityTable.simIndex2) {
            if (item.second.CategoryOrder >= 3) {
                item.second.CategoryOrder--;
            } else if (item.second.CategoryOrder == 2) {
                item.second.CategoryOrder = 0;
            }
        }
        for (auto &item: earlierSimilarityTable.simIndex3) {
            if (item.second.CategoryOrder >= 3) {
                item.second.CategoryOrder--;
            } else if (item.second.CategoryOrder == 2) {
                item.second.CategoryOrder = 0;
            }
        }
        return 0;
    }

    // updated
    int save() {
        printf("------------------------Saving index----------------------\n");
        printf("Saving index..\n");
        uint64_t size;
        FileOperator fileOperator((char *) KVPath.data(), FileOpenType::Write);

        fileOperator.write((uint8_t *) &earlierTable, sizeof(uint64_t) * 2);
        size = earlierTable.fpTable.size();
        fileOperator.write((uint8_t *) &size, sizeof(uint64_t));
        for (auto item: earlierTable.fpTable) {
            fileOperator.write((uint8_t *) &item.first, sizeof(SHA1FP));
            fileOperator.write((uint8_t *) &item.second, sizeof(FPTableEntry));
        }
        printf("earlier table saves %lu items\n", size);
        printf("earlier total size:%lu, duplicate size:%lu\n", earlierTable.totalSize, earlierTable.migrateSize);
        size = earlierSimilarityTable.simIndex1.size();
        fileOperator.write((uint8_t *) &size, sizeof(uint64_t));
        for (auto item: earlierSimilarityTable.simIndex1) {
            fileOperator.write((uint8_t *) &item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t *) &item.second, sizeof(BasePos));
        }
        printf("earlier similar table1 saves %lu items\n", size);
        size = earlierSimilarityTable.simIndex2.size();
        fileOperator.write((uint8_t *) &size, sizeof(uint64_t));
        for (auto item: earlierSimilarityTable.simIndex2) {
            fileOperator.write((uint8_t *) &item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t *) &item.second, sizeof(BasePos));
        }
        printf("earlier similar table2 saves %lu items\n", size);
        size = earlierSimilarityTable.simIndex3.size();
        fileOperator.write((uint8_t *) &size, sizeof(uint64_t));
        for (auto item: earlierSimilarityTable.simIndex3) {
            fileOperator.write((uint8_t *) &item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t *) &item.second, sizeof(BasePos));
        }
        printf("earlier similar table3 saves %lu items\n", size);

        fileOperator.write((uint8_t *) &laterTable, sizeof(uint64_t) * 2);
        size = laterTable.fpTable.size();
        fileOperator.write((uint8_t *) &size, sizeof(uint64_t));
        for (auto item: laterTable.fpTable) {
            fileOperator.write((uint8_t *) &item.first, sizeof(SHA1FP));
            fileOperator.write((uint8_t *) &item.second, sizeof(FPTableEntry));
        }
        printf("later table saves %lu items\n", size);
        printf("later total size:%lu, duplicate size:%lu\n", laterTable.totalSize, laterTable.migrateSize);
        size = laterSimilarityTable.simIndex1.size();
        fileOperator.write((uint8_t *) &size, sizeof(uint64_t));
        for (auto item: laterSimilarityTable.simIndex1) {
            fileOperator.write((uint8_t *) &item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t *) &item.second, sizeof(BasePos));
        }
        printf("later similar table1 saves %lu items\n", size);
        size = laterSimilarityTable.simIndex2.size();
        fileOperator.write((uint8_t *) &size, sizeof(uint64_t));
        for (auto item: laterSimilarityTable.simIndex2) {
            fileOperator.write((uint8_t *) &item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t *) &item.second, sizeof(BasePos));
        }
        printf("later similar table2 saves %lu items\n", size);
        size = laterSimilarityTable.simIndex3.size();
        fileOperator.write((uint8_t *) &size, sizeof(uint64_t));
        for (auto item: laterSimilarityTable.simIndex3) {
            fileOperator.write((uint8_t *) &item.first, sizeof(uint64_t));
            fileOperator.write((uint8_t *) &item.second, sizeof(BasePos));
        }
        printf("later similar table3 saves %lu items\n", size);

        fileOperator.fdatasync();

        return 0;
    }

    int load() {
        printf("-----------------------Loading index-----------------------\n");
        printf("Loading index..\n");
        uint64_t sizeE = 0;
        uint64_t sizeL = 0;
        SHA1FP tempFP;
        FPTableEntry tempFPTableEntry;
        uint64_t tempFeature;
        BasePos tempBasePos;
        FileOperator fileOperator((char *) KVPath.data(), FileOpenType::Read);
        assert(earlierTable.fpTable.size() == 0);
        assert(laterTable.fpTable.size() == 0);

        fileOperator.read((uint8_t *) &earlierTable, sizeof(uint64_t) * 2);
        fileOperator.read((uint8_t *) &sizeE, sizeof(uint64_t));
        for (uint64_t i = 0; i < sizeE; i++) {
            fileOperator.read((uint8_t *) &tempFP, sizeof(SHA1FP));
            fileOperator.read((uint8_t *) &tempFPTableEntry, sizeof(FPTableEntry));
            earlierTable.fpTable.insert({tempFP, tempFPTableEntry});
        }
        printf("earlier table load %lu items\n", sizeE);
        fileOperator.read((uint8_t *) &sizeE, sizeof(uint64_t));
        for (uint64_t i = 0; i < sizeE; i++) {
            fileOperator.read((uint8_t *) &tempFeature, sizeof(uint64_t));
            fileOperator.read((uint8_t *) &tempBasePos, sizeof(BasePos));
            earlierSimilarityTable.simIndex1.insert({tempFeature, tempBasePos});
        }
        printf("earlier similar table1 load %lu items\n", sizeE);
        fileOperator.read((uint8_t *) &sizeE, sizeof(uint64_t));
        for (uint64_t i = 0; i < sizeE; i++) {
            fileOperator.read((uint8_t *) &tempFeature, sizeof(uint64_t));
            fileOperator.read((uint8_t *) &tempBasePos, sizeof(BasePos));
            earlierSimilarityTable.simIndex2.insert({tempFeature, tempBasePos});
        }
        printf("earlier similar table2 load %lu items\n", sizeE);
        fileOperator.read((uint8_t *) &sizeE, sizeof(uint64_t));
        for (uint64_t i = 0; i < sizeE; i++) {
            fileOperator.read((uint8_t *) &tempFeature, sizeof(uint64_t));
            fileOperator.read((uint8_t *) &tempBasePos, sizeof(BasePos));
            earlierSimilarityTable.simIndex3.insert({tempFeature, tempBasePos});
        }
        printf("earlier similar table3 load %lu items\n", sizeE);


        fileOperator.read((uint8_t *) &earlierTable, sizeof(uint64_t) * 2);
        fileOperator.read((uint8_t *) &sizeL, sizeof(uint64_t));
        for (uint64_t i = 0; i < sizeL; i++) {
            fileOperator.read((uint8_t *) &tempFP, sizeof(SHA1FP));
            fileOperator.read((uint8_t *) &tempFPTableEntry, sizeof(FPTableEntry));
            laterTable.fpTable.insert({tempFP, tempFPTableEntry});
        }
        printf("later table load %lu items\n", sizeL);
        fileOperator.read((uint8_t *) &sizeL, sizeof(uint64_t));
        for (uint64_t i = 0; i < sizeL; i++) {
            fileOperator.read((uint8_t *) &tempFeature, sizeof(uint64_t));
            fileOperator.read((uint8_t *) &tempBasePos, sizeof(BasePos));
            laterSimilarityTable.simIndex1.insert({tempFeature, tempBasePos});
        }
        printf("later similar table1 load %lu items\n", sizeL);
        fileOperator.read((uint8_t *) &sizeL, sizeof(uint64_t));
        for (uint64_t i = 0; i < sizeL; i++) {
            fileOperator.read((uint8_t *) &tempFeature, sizeof(uint64_t));
            fileOperator.read((uint8_t *) &tempBasePos, sizeof(BasePos));
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

//    LookupResult localLookup(uint64_t s1, uint64_t s2, uint64_t s3){
//        auto iter1 = localSF1.find(s1);
//        if (iter1 != localSF1.end()) {
//            return LookupResult::Similar;
//        }
//        auto iter2 = localSF2.find(s2);
//        if (iter2 != localSF2.end()) {
//            return LookupResult::Similar;
//        }
//        auto iter3 = localSF3.find(s3);
//        if (iter3 != localSF3.end()) {
//            return LookupResult::Similar;
//        }
//    }
//
//    int localAdd(uint64_t s1, uint64_t s2, uint64_t s3){
//        localSF1.insert(s1);
//        localSF2.insert(s2);
//        localSF3.insert(s3);
//    }
//
//    int localClear(){
//        localSF1.clear();
//        localSF2.clear();
//        localSF3.clear();
//    }
    uint64_t getTotalLength() const {
        return totalLength;
    }

    void setTotalLength(uint64_t totalLength) {
        MetadataManager::totalLength = totalLength;
    }

    uint64_t getAfterDedup() const {
        return afterDedup;
    }

    void setAfterDedup(uint64_t afterDedup) {
        MetadataManager::afterDedup = afterDedup;
    }

    uint64_t getAfterDelta() const {
        return afterDelta;
    }

    void setAfterDelta(uint64_t afterDelta) {
        MetadataManager::afterDelta = afterDelta;
    }

    uint64_t getAfterCompression() const {
        return AfterCompression;
    }

    void setAfterCompression(uint64_t afterCompression) {
        AfterCompression = afterCompression;
    }

private:
    FPIndex earlierTable;
    FPIndex laterTable;
    SimilarityIndex earlierSimilarityTable;
    SimilarityIndex laterSimilarityTable;

    uint64_t totalLength = 0, afterDedup = 0, afterDelta = 0, AfterCompression = 0;

    MutexLock tableLock;
};

static MetadataManager *GlobalMetadataManagerPtr;

#endif //MEGA_MATADATAMANAGER_H