//
// Created by Qiming Lyu on 2022/10/9.
//
#include <stdlib.h>
#include <memory.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"

PageFrame *getFrameByPageNum(BM_BufferPool *const bm, const PageNumber pageNum);
PageFrame *getFreeFrame(BM_BufferPool *const bm);
PageFrame *loadPageFromDiskToFrame(BM_BufferPool *const bm, const PageNumber pageNum);

ListQueue *createQueue();
void freeQueue(ListQueue *queue);

// FIFO
void addNode(ListQueue *queue, struct PageFrame *data);
struct PageFrame *popNode(ListQueue *queue);


// LRU
void addLRUCache(ListQueue *queue, struct PageFrame *data);
void readLRUNode(ListQueue *queue, struct PageFrame *data);
struct PageFrame *getLRUCache(ListQueue *queue);

// LRU_K
void addLRU_KCache(BM_BufferPool *const bm, PageFrame *data);
void readLRU_KNode(BM_BufferPool *const bm, PageFrame *data);
PageFrame *getLRU_kCache(BM_BufferPool *const bm);

void breakList(ListQueue *queue, ListNode *node);
void addToHead(ListQueue *queue, ListNode *node);

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *startData) {
    // init strategy
    switch (strategy) {
        case RS_FIFO:
            bm->listQueue = createQueue();
            bm->kHistory = NULL;
            bm->startData = NULL;
            break;
        case RS_LRU:
            bm->listQueue = createQueue();
            bm->kHistory = NULL;
            bm->startData = NULL;
            break;
        case RS_LRU_K:
            bm->listQueue = createQueue();
            bm->kHistory = malloc(sizeof(int) * numPages);
            memset(bm->kHistory, -1, sizeof(int) * numPages);
            if (startData == NULL) {
                // set default value 1 if startData is null
                startData = (void *) 1;
            }
            bm->startData = startData;
            break;
        default:
            return RC_STRATEGY_NOT_FOUND;
    }

    // check file exist
    SM_FileHandle fileHandle;
    RC file_rc = openPageFile((char *)pageFileName, &fileHandle);
    if (file_rc != RC_OK) {
        return file_rc;
    }
    closePageFile(&fileHandle);

    // init bm
    bm->pageFile = (char *)pageFileName;
    bm->strategy = strategy;
    bm->numPages = numPages;
    bm->mgmtData = calloc(numPages, sizeof(PageFrame));
    bm->numReadIO = 0;
    bm->numWriteIO = 0;

    //create cech
    PageFrame *pageFrames = bm->mgmtData;
    for (int i = 0; i < numPages; ++i) {
        pageFrames[i].dirty = 0;
        pageFrames[i].fixCount = 0;
        pageFrames[i].pageNum = -1;
        pageFrames[i].used = 0;
        pageFrames[i].cache = calloc(PAGE_SIZE, sizeof(char));
    }

    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm) {
    // check parameter
    if (bm->mgmtData == NULL || bm->pageFile == NULL) {
        return RC_NOT_INITIALIZED;
    }

    RC rc_pool = forceFlushPool(bm);
    if (rc_pool != RC_OK) {
        return rc_pool;
    }

    // free strategy
    freeQueue(bm->listQueue);

    PageFrame *frames = bm->mgmtData;
    for (int i = 0; i < bm->numPages; ++i) {
        free(frames[i].cache);
    }

    free(bm->mgmtData);
    if (bm->kHistory != NULL) {
        free(bm->kHistory);
    }

    bm->pageFile = NULL;
    bm->numPages = 0;
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm) {
    // check parameter
    if (bm->mgmtData == NULL || bm->pageFile == NULL) {
        return RC_NOT_INITIALIZED;
    }

    PageFrame *frames = bm->mgmtData;
    BM_PageHandle pageHandle;
    for (int i = 0; i < bm->numPages; ++i) {
        if (frames[i].dirty == 1) {
            pageHandle.pageNum = frames[i].pageNum;
            RC rc_page = forcePage(bm, &pageHandle);
            if (rc_page != RC_OK) {
                return rc_page;
            }
        }
    }
    return RC_OK;
}

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *frame = getFrameByPageNum(bm, page->pageNum);
    if (frame == NULL) {
        return RC_FRAME_NOT_FOUND;
    }

    frame->dirty = 1;
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    // check parameter
    if (bm->mgmtData == NULL || bm->pageFile == NULL) {
        return RC_NOT_INITIALIZED;
    }

    PageFrame *frame = getFrameByPageNum(bm, page->pageNum);
    if (frame == NULL) {
        return RC_FRAME_NOT_FOUND;
    }

    if (frame->fixCount == 0) {
        return RC_FRAME_NOT_PIN;
    }

    frame->fixCount--;
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    // check parameter
    if (page->pageNum < 0) {
        return RC_ILLEGAL_PARAMETER;
    }
    if (bm->mgmtData == NULL || bm->pageFile == NULL) {
        return RC_NOT_INITIALIZED;
    }

    // find page from frames
    PageFrame *frame = getFrameByPageNum(bm, page->pageNum);
    if (frame == NULL) {
        return RC_FRAME_NOT_FOUND;
    }

    if (frame->dirty == 0) {
        return RC_OK;
    }

    SM_FileHandle fileHandle;
    RC rc_open = openPageFile(bm->pageFile, &fileHandle);
    if (rc_open != RC_OK) {
        return rc_open;
    }

    RC rc_write = writeBlock(page->pageNum, &fileHandle, frame->cache);
    if (rc_write != RC_OK) {
        return rc_write;
    }

    RC rc_close = closePageFile(&fileHandle);
    if (rc_close != RC_OK) {
        return rc_close;
    }

    frame->dirty = 0;
    bm->numWriteIO++;
    return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum) {
    // check parameter
    if (pageNum < 0) {
        return RC_ILLEGAL_PARAMETER;
    }
    if (bm->mgmtData == NULL || bm->pageFile == NULL) {
        return RC_NOT_INITIALIZED;
    }

    // find page from frames
    PageFrame  *frame = getFrameByPageNum(bm, pageNum);
    if (frame == NULL) {
        frame = loadPageFromDiskToFrame(bm, pageNum);
        if (frame == NULL) {
            return RC_FRAME_FIXCOUNT_NOT_ZERO;
        }
        bm->numReadIO++;
    } else if (bm->strategy == RS_LRU){
        readLRUNode(bm->listQueue, frame);
    } else if (bm->strategy == RS_LRU_K) {
        readLRU_KNode(bm, frame);
    }

    frame->fixCount++;
    page->pageNum = pageNum;
    page->data = frame->cache;
    return RC_OK;
}

PageNumber *getFrameContents (BM_BufferPool *const bm) {
    // check parameter
    if (bm->mgmtData == NULL || bm->pageFile == NULL) {
        return NULL;
    }

    PageNumber *frameContents = calloc(bm->numPages, sizeof(PageNumber));
    PageFrame  *frames = bm->mgmtData;
    for (int i = 0; i < bm->numPages; ++i) {
        frameContents[i] = frames[i].pageNum;
    }
    return frameContents;
}

bool *getDirtyFlags (BM_BufferPool *const bm) {
    // check parameter
    if (bm->mgmtData == NULL || bm->pageFile == NULL) {
        return NULL;
    }

    bool *dirtyFlags = calloc(bm->numPages, sizeof(bool));
    PageFrame  *frames = bm->mgmtData;
    for (int i = 0; i < bm->numPages; ++i) {
        dirtyFlags[i] = frames[i].dirty;
    }
    return dirtyFlags;
}

int *getFixCounts (BM_BufferPool *const bm) {
    // check parameter
    if (bm->mgmtData == NULL || bm->pageFile == NULL) {
        return NULL;
    }

    int *fixCounts = calloc(bm->numPages, sizeof(int));
    PageFrame  *frames = bm->mgmtData;
    for (int i = 0; i < bm->numPages; ++i) {
        fixCounts[i] = frames[i].fixCount;
    }
    return fixCounts;
}

int getNumReadIO (BM_BufferPool *const bm) {
    return bm->numReadIO;
}

int getNumWriteIO (BM_BufferPool *const bm) {
    return bm->numWriteIO;
}

PageFrame *getFrameByPageNum(BM_BufferPool *const bm, const PageNumber pageNum) {
    PageFrame *pageFrames = bm->mgmtData;
    for (int i = 0; i < bm->numPages; ++i) {
        if (pageFrames[i].used == 1 && pageFrames[i].pageNum == pageNum) {
            return &pageFrames[i];
        }
    }

    return NULL;
}

PageFrame *getFreeFrame(BM_BufferPool *const bm) {
    PageFrame *pageFrames = bm->mgmtData;
    for (int i = 0; i < bm->numPages; ++i) {
        if (pageFrames[i].used == 0) {
            return &pageFrames[i];
        }
    }

    // buffer pool is full replace frame
    PageFrame *frame;
    switch (bm->strategy) {
        case RS_FIFO:
            frame = popNode(bm->listQueue);
            break;
        case RS_LRU:
            frame = getLRUCache(bm->listQueue);
            break;
        case RS_LRU_K:
            frame = getLRU_kCache(bm);
            break;
        default:
            printf("WARN:strategy not found\n");
            exit(1);
    }
    if (frame == NULL) {
        return NULL;
    }

    //write frame if dirty before replaced
    BM_PageHandle pageHandle;
    pageHandle.pageNum = frame->pageNum;
    forcePage(bm, &pageHandle);

    //replace frame
    frame->used = 0;
    memset(frame->cache, 0, PAGE_SIZE);
    return frame;
}

PageFrame *loadPageFromDiskToFrame(BM_BufferPool *const bm, const PageNumber pageNum) {
    PageFrame *frame = getFreeFrame(bm);
    if (frame == NULL) {
        return NULL;
    }

    // loading data from disk to cache
    SM_FileHandle fileHandle;
    RC openFileRC = openPageFile(bm->pageFile, &fileHandle);
    if (openFileRC != RC_OK) {
        printf("ERROR CODE:%d\n", RC_OK);
        exit(1);
    }

    if (fileHandle.totalNumPages -1 < pageNum) {
        ensureCapacity(pageNum, &fileHandle);
    }
    RC readRC = readBlock(pageNum, &fileHandle, frame->cache);
    if (readRC != RC_OK) {
        printf("ERROR CODE:%d\n", RC_OK);
        exit(1);
    }

    switch (bm->strategy) {
        case RS_FIFO:
            addNode(bm->listQueue, frame);
            break;
        case RS_LRU:
            addLRUCache(bm->listQueue, frame);
            break;
        case RS_LRU_K:
            addLRU_KCache(bm, frame);
            break;
        default:
            printf("WARN:strategy not found\n");
            exit(1);
    }

    frame->pageNum = pageNum;
    frame->used = 1;
    closePageFile(&fileHandle);
    return frame;
}

/**
 * queue
 * @return
 */
ListQueue *createQueue() {
    ListQueue *queue = calloc(1, sizeof(ListQueue));
    queue->numNodes = 0;

    return queue;
}

void freeQueue(ListQueue *queue) {
    ListNode *node = queue->head;
    ListNode *next;
    while (node != NULL) {
        next = node->next;
        free(node);
        node = next;
    }

    free(queue);
}

/**
 * FIFO
 * @return
 */
void addNode(ListQueue *queue, struct PageFrame *data) {
    ListNode *node = calloc(1, sizeof(ListNode));
    node->data = data;
    if (queue->numNodes == 0) {
        queue->head = node;
        queue->tail = node;
    } else {
        node->prev = queue->tail;
        queue->tail->next = node;
        queue->tail = node;
    }
    queue->numNodes++;
}

PageFrame *popNode(ListQueue *queue) {
    if (queue->numNodes < 1) {
        return NULL;
    }

    ListNode *node = queue->head;
    while (node != NULL) {
        if (node->data->fixCount != 0) {
            node = node->next;
            continue;
        }

        breakList(queue, node);
        queue->numNodes--;
        struct PageFrame *pFrame = node->data;
        free(node);
        return pFrame;
    }

    return NULL;
}

void breakList(ListQueue *queue, ListNode *node) {
    ListNode *next = node->next;
    ListNode *pre = node->prev;
    if (pre == NULL) {
        queue->head = next;
        next->prev = NULL;
    } else if (next == NULL) {
        queue->tail = pre;
        pre->next = NULL;
    } else {
        pre->next = next;
        next->prev = pre;
    }
}

void addToHead(ListQueue *queue, ListNode *node) {
    node->next = queue->head;
    queue->head->prev = node;
    queue->head = node;
}

/**
 * LRU
 */
void addLRUCache(ListQueue *queue, struct PageFrame *data) {
    ListNode *node = calloc(1, sizeof(ListNode));
    node->data = data;

    if (queue->numNodes == 0) {
        queue->head = node;
        queue->tail = node;
    } else {
        addToHead(queue, node);
    }

    queue->numNodes++;
}

void readLRUNode(ListQueue *queue, struct PageFrame *data) {
    ListNode *node = queue->head;
    while (node != NULL) {
        if (node->data->pageNum != data->pageNum) {
            node = node->next;
            continue;
        }

        //break list
        breakList(queue, node);
        // move to head
        addToHead(queue, node);
        break;
    }
}

struct PageFrame *getLRUCache(ListQueue *queue) {
    if (queue->numNodes < 1) {
        return NULL;
    }

    ListNode *node = queue->tail;
    while (node != NULL) {
        if (node->data->fixCount != 0) {
            node = node->prev;
            continue;
        }

        breakList(queue, node);
        queue->numNodes--;
        struct PageFrame *pFrame = node->data;
        free(node);
        return pFrame;
    }

    return NULL;
}

// LRU_K
int getFrameIndex(PageFrame *frameArray, int size, PageFrame *data) {
    for (int i = 0; i < size; ++i) {
        if (frameArray[i].pageNum == data->pageNum) {
            return i;
        }
    }

    return -1;
}

void addLRU_KCache(BM_BufferPool *const bm, PageFrame *data) {
    ListNode *node = calloc(1, sizeof(ListNode));
    node->data = data;

    if (bm->listQueue->numNodes == 0) {
        bm->listQueue->head = node;
        bm->listQueue->tail = node;
    } else {
        addToHead(bm->listQueue, node);
    }

    bm->listQueue->numNodes++;
    bm->kHistory[getFrameIndex(bm->mgmtData, bm->numPages, data)]++;
}

void readLRU_KNode(BM_BufferPool *const bm, struct PageFrame *data) {
    int k = (int) bm->startData;
    int history = bm->kHistory[getFrameIndex(bm->mgmtData, bm->numPages, data)];
    if (++bm->kHistory[history] < k) {
        return;
    }

    ListNode *node = bm->listQueue->head;
    while (node != NULL) {
        if (node->data->pageNum != data->pageNum) {
            node = node->next;
            continue;
        }

        //break list
        breakList(bm->listQueue, node);
        // move to head
        addToHead(bm->listQueue, node);
        break;
    }
    bm->kHistory[getFrameIndex(bm->mgmtData, bm->numPages, data)] = 0;
}

PageFrame *getLRU_kCache(BM_BufferPool *const bm) {
    if (bm->listQueue->numNodes < 1) {
        return NULL;
    }

    int k = (int) bm->startData;
    ListNode *node = bm->listQueue->tail;
    ListNode *min = NULL;
    while (node != NULL) {
        if (node->data->fixCount != 0) {
            node = node->prev;
            continue;
        }

        int nodeIndex = getFrameIndex(bm->mgmtData, bm->numPages, node->data);
        if (bm->kHistory[nodeIndex] < k) {
            breakList(bm->listQueue, node);
            bm->listQueue->numNodes--;
            bm->kHistory[nodeIndex] = -1;
            return node->data;
        }

        if (min == NULL) {
            min = node;
            node = node->prev;
            continue;
        }

        int minIndex = getFrameIndex(bm->mgmtData, bm->numPages, min->data);
        int minHistory = bm->kHistory[minIndex];
        int nodeHistory = bm->kHistory[nodeIndex];
        if (nodeHistory < minHistory) {
            min = node;
        }
        node = node->prev;
    }

    breakList(bm->listQueue, min);
    bm->listQueue->numNodes--;
    bm->kHistory[getFrameIndex(bm->mgmtData, bm->numPages, min->data)] = -1;
    return min->data;
}
