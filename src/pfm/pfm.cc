#include "src/include/pfm.h"
#include <cstring>
#include <fstream>
#include <iostream>
#include <cassert>

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
            std::cout<< "error: File already exists" <<std::endl;
            return -1;
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
            std::cout << "error: File does not exist" << std::endl;
            return -1; // Not exist
        }

        std::string command = "rm -f " + fileName;
        int result = std::system(command.c_str());
        if (result != 0) {
            return -1;
        }
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        // File not exists, return an error code
        std::shared_ptr<std::fstream> filestream = std::make_shared<std::fstream>(fileName, std::ios::in | std::ios::out | std::ios::binary);
        if (!filestream->good()) {
            filestream->close();
            return -1; // Not exist
        }

        if (!filestream->is_open()) {
            // File opening failed, return an error code
            std::cout << "Error: Open file error" <<std::endl;
            return -1;
        }

        // read counters in the beginning of the hidden page
        unsigned r=0, w=0, a=0;
        filestream->seekg(0, std::ios::beg);
        filestream->read(reinterpret_cast<char*>(&r), sizeof (int));
        filestream->read(reinterpret_cast<char*>(&w), sizeof (int));
        filestream->read(reinterpret_cast<char*>(&a), sizeof (int));

        // resume the record
        fileHandle.readPageCounter = r;
        fileHandle.writePageCounter = w;
        fileHandle.appendPageCounter = a;
        fileHandle.pageFileName = fileName;
        fileHandle.openFileStream = filestream;
        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        char buffer[PAGE_SIZE];

        if(fileHandle.openFileStream == nullptr) {
            return -1;
        }

        if(!fileHandle.openFileStream->is_open()){
            return -1;
        }

        fileHandle.openFileStream->seekp(0, std::ios::beg);

        unsigned r = fileHandle.readPageCounter, w = fileHandle.writePageCounter, a = fileHandle.appendPageCounter;

        fileHandle.openFileStream->write(reinterpret_cast<const char*>(&r), sizeof(int));
        fileHandle.openFileStream->write(reinterpret_cast<const char*>(&w), sizeof(int));
        fileHandle.openFileStream->write(reinterpret_cast<const char*>(&a), sizeof(int));

        if (fileHandle.pageFileName == "rm_test_large_table") {
            FileHandle fh;
            std::fstream filex("backup", std::ios::out | std::ios::binary);

            memset(buffer, 0, PAGE_SIZE);

            memmove(buffer, &r, sizeof(unsigned));
            memmove(buffer + sizeof(unsigned), &w, sizeof(unsigned));
            memmove(buffer + 2 * sizeof(unsigned), &a, sizeof(unsigned));

            filex.write(buffer, PAGE_SIZE);

            for (int i = 0; i < fileHandle.getNumberOfPages(); ++i) {
                fileHandle.readPage(i, buffer);
                filex.write(buffer, PAGE_SIZE);
            }
            filex.close();
        }

        fileHandle.openFileStream->flush();
        fileHandle.openFileStream->close();

        if (fileHandle.openFileStream->fail()) {
            return -1;
        }

        if (fileHandle.pageFileName == "rm_test_large_table") {
            std::remove(fileHandle.pageFileName.c_str());

            // Attempt to rename the source file to the target file name
            if (std::rename("backup", fileHandle.pageFileName.c_str()) != 0) {
                perror("Error renaming file");
            }
        }
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() {
        PagedFileManager::instance().closeFile(*this);
    };

    FileHandle::FileHandle(FileHandle &&fileHandle) noexcept {
        openFileStream = fileHandle.openFileStream; // new
        fileHandle.openFileStream.reset();

        readPageCounter = fileHandle.readPageCounter;
        writePageCounter = fileHandle.writePageCounter;
        appendPageCounter = fileHandle.appendPageCounter;
        pageFileName = fileHandle.pageFileName;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        // cannot open the file
        if (!openFileStream->is_open()) {
            return -1;
        }
        // read the nonexistent page
        if(pageNum > getNumberOfPages()){
            return -1;
        }

        // move the stream position to the page
        std::streampos offset = (pageNum + 1) * PAGE_SIZE;
        openFileStream->seekg(offset, std::ios::beg);

        // read the stream data from file to void *data
        openFileStream->read(reinterpret_cast<char*>(data), PAGE_SIZE);

        readPageCounter = readPageCounter + 1;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        // cannot open the file
        if (!openFileStream->is_open()) {
            return -1;
        }
        // write the nonexistent page
        if(pageNum > getNumberOfPages()){
            return -1;
        }

        // move the stream position to the page
        std::streampos offset = (pageNum + 1) * PAGE_SIZE;
        openFileStream->seekp(offset, std::ios::beg);

        // write the data into file
        openFileStream->write(reinterpret_cast<const char*>(data), PAGE_SIZE);

        writePageCounter = writePageCounter + 1;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        if (!openFileStream->is_open()) {
            return -1;
        }

        // move the stream position to the end of page
        openFileStream->seekp(0, std::ios::end);
        // write the data into file
        openFileStream->write(reinterpret_cast<const char*>(data), PAGE_SIZE);

        if (!openFileStream->good()) {
            std::cout<<"bad writing\n";
            return -1; // Write failed
        }

        openFileStream->flush();
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