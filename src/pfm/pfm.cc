#include <filesystem>
#include "src/include/pfm.h"

namespace PeterDB {
    using std::ofstream;
    using std::fstream;
    using std::ios;
    namespace fs = std::filesystem;

    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        (void)this;
        if (fs::exists(fileName)) {
            return -1;
        }

        ofstream file(fileName, ios::binary);

        if (!file.is_open()) {
            return -1;
        }
        constexpr std::array<char, PAGE_SIZE> emptyPage{};
        file.write(emptyPage.data(), PAGE_SIZE);

        file.close();
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        (void)this;
        try {
            if (!fs::remove(fileName)) {
                return -1;
            }
        } catch (const fs::filesystem_error& e) {
            return -1;
        }
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        (void)this;
        return fileHandle.open(fileName);
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        (void)this;
        return fileHandle.close();
    }


    /// Begin of FileHandle

    FileHandle::FileHandle(FileHandle &&fileHandle) noexcept {
        swap(*this, fileHandle);
    }

    FileHandle &FileHandle::operator=(FileHandle &&fileHandle) noexcept {
        swap(*this, fileHandle);
        return *this;
    }

    void swap(FileHandle &lhs, FileHandle &rhs) {
        using std::swap;
        swap(lhs.readPageCounter, rhs.readPageCounter);
        swap(lhs.writePageCounter, rhs.writePageCounter);
        swap(lhs.appendPageCounter, rhs.appendPageCounter);
        swap(lhs.file, rhs.file);
        swap(lhs.pageCount, rhs.pageCount);
        swap(lhs.freeSpaces, rhs.freeSpaces);
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        readPageCounter += 1;
        if (pageNum >= pageCount) {

            return -2;
        }

        if (file.fail()) {

            return -1;
        }

        file.seekg(pageOffset(pageNum));
        file.read(static_cast<char *>(data), PAGE_SIZE);
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {

        writePageCounter += 1;
        if (pageNum >= pageCount) {

            return -2;
        }

        if (file.fail()) {

            return -1;
        }

        file.seekp(pageOffset(pageNum));
        file.write(static_cast<const char *>(data), PAGE_SIZE);
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {

        appendPageCounter += 1;
        if (file.fail()) {

            return -1;
        }

        file.seekp(0, ios::end);
        file.write(static_cast<const char *>(data), PAGE_SIZE);
        file.flush();

        pageCount += 1;

        return 0;
    }

    unsigned FileHandle::getNumberOfPages() const {
        return pageCount;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                        unsigned &appendPageCount) const {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

    RC FileHandle::open(const std::string &fileName) {
        file = fstream(fileName, ios::in | ios::out | ios::binary);
        if (!file.is_open()) {

            return -1;
        }

        Metadata metadata;
        file.read(reinterpret_cast<char *>(&metadata), sizeof(Metadata));

        pageCount = metadata.pageCount;
        readPageCounter = metadata.readPageCounter;
        writePageCounter = metadata.writePageCounter;
        appendPageCounter = metadata.appendPageCounter;


        return 0;
    }

    RC FileHandle::close() {
        if (!file.is_open()) {

            return -1;
        }

        Metadata metadata;
        metadata.pageCount = pageCount;
        metadata.readPageCounter = readPageCounter;
        metadata.writePageCounter = writePageCounter;
        metadata.appendPageCounter = appendPageCounter;

        file.seekp(0, ios::beg);
        file.write(reinterpret_cast<const char *>(&metadata), sizeof(Metadata));

        file.close();
        if (file.fail()) {

            return -1;
        }
        return 0;
    }

    std::streampos FileHandle::pageOffset(PageNum pageNum) {
        // the first 4096 bytes are used to storage metadata, so first page starts at 4096.
        return static_cast<std::streampos>(metadataSize + pageNum * PAGE_SIZE);
    }

} // namespace PeterDB