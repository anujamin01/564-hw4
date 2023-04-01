#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

/*
Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk. Returns
BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O layer returned an
error when a dirty page was being written to disk, and OK otherwise. This private method will get called
by the readPage() and allocPage() methods described below.
Make sure that if the buffer frame allocated has a valid page in it, you remove the appropriate entry from
the hash table.
*/
const Status BufMgr::allocBuf(int &frame)
{
    // iterate through the clock algorithm twice and return bufferexceeded if all buffer frames are not available
    for (int i = 0; i < numBufs * 2; i++)
    {
        this->advanceClock();
        // if valid bit is false
        if (bufTable[clockHand].valid == false)
        {
            return OK;
        }
        // if valid bit is true
        // we check if refbit is set to true
        if (bufTable[clockHand].refbit)
        {
            // it is true, so we clear refbit
            bufTable[clockHand].refbit = false;
            // advance clock again by skipping the loop
            continue;
        }
        // if refbit is false, check is page pinned?
        if (bufTable[clockHand].pinCnt > 0)
        {
            // page is pinned, run clock algo again by skipping the loop
            continue;
        }
        // check dirty bit is set, if it is true
        if (bufTable[clockHand].dirty)
        {
            // we flush page to disk
            // if flushFile returns anything that is not OK, we return UNIXERR
            if (flushFile(bufTable[clockHand].file) != OK)
            {
                return UNIXERR;
            }
        }
        return OK;
    }
    return BUFFEREXCEEDED;
}

/*
First check whether the page is already in the buffer pool by invoking the lookup() method on the
hashtable to get a frame number. There are two cases to be handled depending on the outcome of the
lookup() call:
Case 1. The page is not in the buffer pool. Call allocBuf() to allocate a buffer frame and then call the
method file->readPage() to read the page from the disk into the buffer pool frame. Next, insert the page
into the hashtable. Finally, invoke Set() on the frame to set it up properly. Set() will leave the pinCnt for
the page set to 1. Return a pointer to the frame containing the page via the page parameter.
Case 2. The page is in the buffer pool. In this case set the appropriate refbit, increment the pinCnt for the
page, and then return a pointer to the frame containing the page via the page parameter.
Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer
frames are pinned, HASHTBLERROR if a hash table error occurred.
*/
const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    // case 1: page is not in buffer pool
    int frameNo;
    if (hashTable->lookup(file, PageNo, frameNo) == HASHNOTFOUND)
    {
        Status t = allocBuf(frameNo); // try to allocate buffer frame
        if (t != OK)
        {
            return t; // some error must have occurred
        }

        if (file->readPage(PageNo, page) != OK)
        { // try to read page from disk into the buffer pool frame
            return UNIXERR;
        }

        // insert page into hashtable
        if (hashTable->insert(file, PageNo, frameNo) == HASHTBLERROR)
        {
            return HASHTBLERROR; // error inserting page into the the table
        }
        // invoke set() on the frame to set it up properly
        bufTable[frameNo].Set(file, PageNo);
    }
    else
    { // case 2: page is in buffer pool
        // page already in buffer, inc pincnt and set refbit
        bufTable[frameNo].pinCnt++;
        bufTable[frameNo].refbit = true;
        page = &bufPool[frameNo]; // update where the page is (may or may not have changed)
        // return a pointer to the frame containing the page via the page parameter.
    }
    return OK; // No errors occurred
}

/*
Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true sets the dirty bit.
Returns OK if no errors occurred, HASHNOTFOUND if the page is not in the buffer pool hash table,
PAGENOTPINNED if the pin count is already 0
*/
const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
    int frameNo;

    // if frame exists
    if (hashTable->lookup(file, PageNo, frameNo))
    {
        // return error if page is not pinned
        if (bufTable[PageNo].pinCnt <= 0)
        {
            return PAGENOTPINNED;
        }
        else
        {
            // decrement pin count
            bufTable[frameNo].pinCnt--;
            // set diry bit
            if (dirty)
            {
                bufTable[frameNo].dirty = true;
            }
            return OK;
        }
    }
    // frame doesn't exist
    return HASHNOTFOUND;
}

/*
This call is kind of weird. The first step is to allocate an empty page in the specified file by invoking the
file->allocatePage() method. This method will return the page number of the newly allocated page. Then
allocBuf() is called to obtain a buffer pool frame. Next, an entry is inserted into the hash table and Set() is
invoked on the frame to set it up properly. The method returns both the page number of the newly
allocated page to the caller via the pageNo parameter and a pointer to the buffer frame allocated for the
page via the page parameter. Returns OK if no errors occurred, UNIXERR if a Unix error occurred,
BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table error
occurred.
*/
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    int frameNo;
    // pageNo is newly allocated
    file->allocatePage(pageNo);
    // call allocBuf to obtain a buffer pool frame
    Status s = allocBuf(frameNo);
    if (s != OK){
        return s;
    }
    // insert page into hashtable
    if (hashTable->insert(file, pageNo, frameNo) == HASHTBLERROR)
    {
        return HASHTBLERROR; // error inserting page into the the table
    }
    // invoke set() on the frame to set it up properly
    bufTable[frameNo].Set(file, pageNo);
    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
