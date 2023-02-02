//
// Created by qiming lyu on 2022/9/8.
//
#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void initStorageManager(void) {
    printf("init storage manager...\n");
}

RC createPageFile(char *fileName) {
    // Get the file pointer
    FILE *pFile = fopen(fileName, "wb+");
    if (pFile == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // Initialize page and write to file
    SM_PageHandle page_handle = malloc(sizeof(char) * PAGE_SIZE);
    memset(page_handle, 0, PAGE_SIZE);
    size_t count = fwrite(page_handle, sizeof(char), PAGE_SIZE, pFile);
    if (count != PAGE_SIZE) {
		free(page_handle);
		fclose(pFile);
        return RC_WRITE_FAILED;
    }

    fclose(pFile);
    free(page_handle);
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    // Get the file pointer
    FILE *pFile = fopen(fileName, "rb+");
    if (pFile == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // Configure file handle
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;
    fseek(pFile, 0L, SEEK_END);
    fHandle->totalNumPages = (int)(ftell(pFile) / PAGE_SIZE);
    fHandle->mgmtInfo = pFile;
    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 1) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    fclose(fHandle->mgmtInfo);
    return RC_OK;
}

RC destroyPageFile(char *fileName) {
    FILE *pFile = fopen(fileName, "wb");
    if (pFile == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    fclose(pFile);
    remove(fileName);
    return RC_OK;
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check file handle
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 1) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Check validity of page number
    if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Jump to the beginning of the file
    int i = fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET);
    if (i != 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Read to memory
    fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
    // Set the offset pointing to the current block 
    fHandle->curPagePos = pageNum;
    return RC_OK;
}

int getBlockPos(SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check file handle
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 1) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Check validity of page number
    if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Jump to the beginning of the file
    int i = fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET);
    if (i != 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    // Write to file from memory page
    fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);

    return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 1) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // create empty block
    SM_PageHandle memPage[PAGE_SIZE];
    memset(&memPage, 0, PAGE_SIZE);
    // Jump to end of file
    fseek(fHandle->mgmtInfo, 0L, SEEK_END);
    // write empty block to end of file
    fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
    // Increment the total number of pages
    fHandle->totalNumPages += 1;

    return RC_OK;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 1) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    // If the number of pages is larger than the total pages of the file,
    // append an empty block to the file
    int num = numberOfPages - fHandle->totalNumPages; 
    if (num > 0) {
        for (int i = 0; i < num; i++) {
            appendEmptyBlock(fHandle);
        }
    }

    return RC_OK;
}
