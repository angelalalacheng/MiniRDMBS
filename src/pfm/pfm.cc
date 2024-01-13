#include "src/include/pfm.h"

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        fs::path filePath(fileName);

        if (fs::exists(filePath)) {
            // File already exists, return an appropriate error code
            return -1;
        }

        // Create the file using std::ofstream
        std::ofstream file(filePath);

        if (file.is_open()) {
            // File created successfully, close the file
            file.close();
            return 0;
        } else {
            // Failed to create the file, return an appropriate error code
            return -1;
        }

    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        fs::path filePath(fileName);

        if (!fs::exists(filePath)) {
            // File not exists, return an appropriate error code
            return -1;
        }

        fs::remove(filePath);
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if(fs::is_regular_file(fileName)) return -1;
        return -1;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return -1;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        return -1;
    }

    unsigned FileHandle::getNumberOfPages() {
        return -1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }

} // namespace PeterDB