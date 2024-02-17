#ifndef _pfm_h_
#define _pfm_h_

constexpr unsigned PAGE_SIZE = 4096;

#include <string>
#include <fstream>
#include <vector>

namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file

    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    };

    class FileHandle {
    public:
        // variables to keep the counter for each operation
        unsigned readPageCounter = 0;
        unsigned writePageCounter = 0;
        unsigned appendPageCounter = 0;

        FileHandle() = default;                                             // Default constructor
        ~FileHandle() { close(); }                                          // Destructor

        FileHandle(const FileHandle &fileHandle) = delete;
        FileHandle &operator=(const FileHandle &fileHandle) = delete;

        FileHandle(FileHandle &&fileHandle) noexcept;
        FileHandle &operator=(FileHandle &&fileHandle) noexcept;

        friend void swap(FileHandle &lhs, FileHandle &rhs);

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages() const;                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount) const;                 // Put current counter values into variables

        RC open(const std::string &fileName);
        RC close();

        bool isAvailable() const {
            return file.is_open();
        }

        static std::streampos pageOffset(PageNum pageNum);
        std::vector<size_t> freeSpaces;

    protected:
        std::fstream file;

#pragma pack(1)
        struct Metadata {
            PageNum pageCount = 0;
            unsigned readPageCounter = 0;
            unsigned writePageCounter = 0;
            unsigned appendPageCounter = 0;
        };
#pragma pack()

        static constexpr unsigned metadataSize = PAGE_SIZE;
        PageNum pageCount = 0;
    };

} // namespace PeterDB

#endif // _pfm_h_