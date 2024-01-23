#include "src/include/pfm.h"
#include <cstring>

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
        // File already exists, return an error code
        std::ifstream check(fileName);
        if (check.good()) {
            check.close();
            return -1; // File already exists
        }

        // create the file using std::ofstream
        std::ofstream file(fileName , std::ios::binary);

        // preallocate the size
        file.seekp(PAGE_SIZE - 1, std::ios::beg);
        file.write("", 1);

        // write the initial value to the beginning of file
        file.seekp(0, std::ios::beg);
        unsigned initial[] = {0, 0, 0};
        file.write(reinterpret_cast<char*>(initial), sizeof(initial));

        if (file.is_open()) {
            // File created successfully, close the file
            file.close();
            return 0;
        } else {
            // Failed to create the file, return an error code
            return -1;
        }
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        // File not exists, return an error code
        std::ifstream check(fileName);
        if (!check.good()) {
            check.close();
            return -1; // Not exist
        }

        std::__fs::filesystem::remove(fileName);
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        // File not exists, return an error code
        std::ifstream check(fileName);
        if (!check.good()) {
            check.close();
            return -1; // Not exist
        }

        std::fstream file(fileName, std::ios::in | std::ios::out | std::ios::binary);

        if (!file.is_open()) {
            // File opening failed, return an error code
            return -1;
        }

        // read counters in the beginning of the hidden page
        unsigned r=0, w=0, a=0;
        file.seekg(0, std::ios::beg);
        file.read(reinterpret_cast<char*>(&r), sizeof (int));
        file.read(reinterpret_cast<char*>(&w), sizeof (int));
        file.read(reinterpret_cast<char*>(&a), sizeof (int));

        // resume the record
        fileHandle.readPageCounter = r;
        fileHandle.writePageCounter = w;
        fileHandle.appendPageCounter = a;
        fileHandle.pageFileName = fileName;

        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        // All the file's pages are flushed to disk when the file is closed.????

        std::fstream file(fileHandle.pageFileName, std::ios::in | std::ios::out | std::ios::binary);
        file.seekp(0, std::ios::beg);

        unsigned r = fileHandle.readPageCounter, w = fileHandle.writePageCounter, a = fileHandle.appendPageCounter;

        file.write(reinterpret_cast<const char*>(&r), sizeof(int));
        file.write(reinterpret_cast<const char*>(&w), sizeof(int));
        file.write(reinterpret_cast<const char*>(&a), sizeof(int));

        file.flush();
        file.close();

        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        std::fstream file(pageFileName, std::ios::in | std::ios::out | std::ios::binary);

        // cannot open the file
        if (!file.is_open()) {
            return -1;
        }
        // read the nonexistent page
        if(pageNum > getNumberOfPages()){
            return -1;
        }

        // move the stream position to the page
        std::streampos offset = (pageNum + 1) * PAGE_SIZE;
        file.seekg(offset, std::ios::beg);

        // read the stream data from file to void *data
        file.read(reinterpret_cast<char*>(data), PAGE_SIZE);

        file.close();
        readPageCounter = readPageCounter + 1;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        std::fstream file(pageFileName, std::ios::in | std::ios::out | std::ios::binary);

        // cannot open the file
        if (!file.is_open()) {
            return -1;
        }
        // write the nonexistent page
        if(pageNum > getNumberOfPages()){
            return -1;
        }

        // move the stream position to the page
        std::streampos offset = (pageNum + 1) * PAGE_SIZE;
        file.seekp(offset, std::ios::beg);

        // write the data into file
        file.write(reinterpret_cast<const char*>(data), PAGE_SIZE);

        file.close();
        writePageCounter = writePageCounter + 1;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        std::fstream file(pageFileName, std::ios::in | std::ios::out | std::ios::binary);

        if (!file.is_open()) {
            return -1;
        }

        // move the stream position to the end of page
        file.seekg (0, std::ios::end);

        // write the data into file
        file.write(reinterpret_cast<const char*>(data), PAGE_SIZE);

        file.close();
        appendPageCounter = appendPageCounter + 1;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return appendPageCounter;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        // Copy the counter values to the provided variables
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

} // namespace PeterDB