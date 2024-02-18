#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#include <string>
#include <fstream>
#include <memory>

namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance(page is the basic unit)

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fiName);                          // Destroy a file
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
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
        std::string pageFileName;
        std::shared_ptr<std::fstream> openFileStream;
        // add vector to record the free space

        FileHandle();                                                       // Default constructor
        ~FileHandle();
        // Destructor
        FileHandle(FileHandle &&fileHandle) noexcept;

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page(pageNum data save to data space)
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page(write data to pageNum page)
        RC appendPage(const void *data);                                    // Append a specific page to the end of file
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values into variables
    };

} // namespace PeterDB

#endif // _pfm_h_

// append: need to know size of your file (maintain by yourself in hidden layer)
// read and write only happened in one page no sequential
// same content just cover the origin