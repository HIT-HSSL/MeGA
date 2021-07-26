/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_ELIMINATOR_H
#define MEGA_ELIMINATOR_H

DEFINE_uint64(EliminateReadBuffer,
67108864, "Read buffer size for eliminating old version");

extern std::string LogicFilePath;
extern std::string ClassFilePath;
extern std::string VersionFilePath;
extern std::string ClassFileAppendPath;

class Eliminator {
public:
    Eliminator() {

    }

    int run(uint64_t maxVersion) {
        printf("start to eliminate\n");

        printf("delete invalid LC-groups\n");
        versionFileDeleter(1);

        printf("processing categories files\n");
        activeFileCombinationProcessor(1, 2, maxVersion);
        for (uint64_t i = 3; i <= maxVersion; i++) {
            classFileProcessor(i, maxVersion);
        }

        printf("processing volume files\n");
        for (uint64_t i = 2; i <= maxVersion - 1; i++) {
            archivedFileCombinationProcessor(1, 2, i);
            versionFileProcessor(i);
        }

        printf("processing recipe files\n");
        for (uint64_t i = 2; i <= maxVersion; i++) {
            recipeFilesProcessor(i);
        }
        GlobalMetadataManagerPtr->similarityTableMerge();
        printf("Similarity Feature Tables have been updated..\n");
        printf("finish, the earliest version has been eliminated..\n");
    }

private:
    int recipeFilesProcessor(uint64_t recipeId) {
        // rolling back serial number of recipes
        sprintf(oldPath, LogicFilePath.data(), recipeId);
        sprintf(newPath, LogicFilePath.data(), recipeId - 1);
        rename(oldPath, newPath);

        return 0;
    }

    int versionFileProcessor(uint64_t versionId) {
        // append first two archived categories in volumes.

        for (int i = 3; i <= versionId; i++) {
            uint64_t cid = 0;
            while (1) {
                sprintf(oldPath, VersionFilePath.data(), i, versionId, cid);
                FileOperator fileOperator(oldPath, FileOpenType::TRY);
                if (!fileOperator.ok()) {
                    break;
                }
                sprintf(newPath, VersionFilePath.data(), i - 1, versionId - 1, cid);
                rename(oldPath, newPath);
                cid++;
            }
        }

        return 0;
    }

    int versionFileDeleter(uint64_t versionId) {
        for (int i = 1; i <= versionId; i++) {
            uint64_t cid = 0;
            while (1) {
                sprintf(oldPath, VersionFilePath.data(), i, versionId, cid);
                {
                    FileOperator fileOperator(oldPath, FileOpenType::TRY);
                    if (!fileOperator.ok()) {
                        break;
                    }
                }
                remove(oldPath);
                cid++;
            }
        }
        return 0;
    }

    int classFileProcessor(uint64_t classId, uint64_t maxVersion) {
        // rolling back serial number of categories
        uint64_t cid = 0;
        while (1) {
            sprintf(oldPath, ClassFilePath.data(), classId, maxVersion, cid);
            FileOperator fileOperator(oldPath, FileOpenType::TRY);
            if (!fileOperator.ok()) {
                break;
            }
            sprintf(newPath, ClassFilePath.data(), classId - 1, maxVersion - 1, cid);
            rename(oldPath, newPath);
            cid++;
        }
        return 0;
    }

    int activeFileCombinationProcessor(uint64_t classId1, uint64_t classId2, uint64_t maxVersion) {
        // rolling back serial number of categories
        // append first two active categories.
        uint64_t cid = 0;
        while (1) {
            sprintf(oldPath, ClassFilePath.data(), classId1, maxVersion, cid);
            FileOperator fileOperator(oldPath, FileOpenType::TRY);
            if (!fileOperator.ok()) {
                break;
            }
            sprintf(newPath, ClassFilePath.data(), classId1, maxVersion - 1, cid);
            rename(oldPath, newPath);
            cid++;
        }

        cid = 0;
        while (1) {
            sprintf(oldPath, ClassFilePath.data(), classId2, maxVersion, cid);
            FileOperator fileOperator(oldPath, FileOpenType::TRY);
            if (!fileOperator.ok()) {
                break;
            }
            sprintf(newPath, ClassFileAppendPath.data(), classId1, maxVersion - 1, cid);
            rename(oldPath, newPath);
            cid++;
        }

        return 0;
    }

    int archivedFileCombinationProcessor(uint64_t classId1, uint64_t classId2, uint64_t version) {
        // rolling back serial number of categories
        // append first two active categories.
        uint64_t cid = 0;
        while (1) {
            sprintf(oldPath, VersionFilePath.data(), classId1, version, cid);
            FileOperator fileOperator(oldPath, FileOpenType::TRY);
            if (!fileOperator.ok()) {
                break;
            }
            sprintf(newPath, VersionFilePath.data(), classId1, version - 1, cid);
            rename(oldPath, newPath);
            cid++;
        }

        uint64_t acid = 0;
        while (1) {
            sprintf(oldPath, VersionFilePath.data(), classId2, version, acid);
            FileOperator fileOperator(oldPath, FileOpenType::TRY);
            if (!fileOperator.ok()) {
                break;
            }
            sprintf(newPath, VersionFilePath.data(), classId1, version - 1, cid);
            rename(oldPath, newPath);
            acid++;
            cid++;
        }

        return 0;
    }

    char oldPath[256];
    char newPath[256];
};

#endif //MEGA_ELIMINATOR_H
