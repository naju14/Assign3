#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

typedef struct BM_PageFrame {
	PageNumber pageNum;
	char *data;
	bool dirty;
	int fixCount;
	int lastUsed;
	int accessCount;
	bool refBit;
} BM_PageFrame;

typedef struct BM_MgmtData {
	BM_PageFrame *frames;
	SM_FileHandle *fileHandle;
	int numReadIO;
	int numWriteIO;
	int *fifoQueue;
	int fifoFront;
	int fifoRear;
	int clockHand;
} BM_MgmtData;

static inline int findFrame(BM_BufferPool *const bm, PageNumber pageNum)
{
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	BM_PageFrame *frames = mgmtData->frames;
	for (int i = 0; i < bm->numPages; i++)
		if (frames[i].pageNum == pageNum) return i;
	return -1;
}

static inline int findFreeFrame(BM_BufferPool *const bm)
{
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	BM_PageFrame *frames = mgmtData->frames;
	for (int i = 0; i < bm->numPages; i++)
		if (frames[i].pageNum == NO_PAGE) return i;
	return -1;
}

static int evictFIFO(BM_BufferPool *const bm)
{
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	for (int attempts = 0; attempts < bm->numPages; attempts++)
	{
		int frameIndex = mgmtData->fifoQueue[mgmtData->fifoFront];
		mgmtData->fifoFront = (mgmtData->fifoFront + 1) % bm->numPages;
		if (mgmtData->frames[frameIndex].fixCount == 0)
		{
			if (mgmtData->frames[frameIndex].dirty)
			{
				writeBlock(mgmtData->frames[frameIndex].pageNum, mgmtData->fileHandle, mgmtData->frames[frameIndex].data);
				mgmtData->numWriteIO++;
			}
			return frameIndex;
		}
	}
	return -1;
}

static int evictLRU(BM_BufferPool *const bm)
{
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	int oldestFrame = -1, oldestTime = INT_MAX;
	for (int i = 0; i < bm->numPages; i++)
	{
		if (mgmtData->frames[i].fixCount == 0 && mgmtData->frames[i].pageNum != NO_PAGE)
		{
			if (mgmtData->frames[i].lastUsed < oldestTime)
			{
				oldestTime = mgmtData->frames[i].lastUsed;
				oldestFrame = i;
			}
		}
	}
	if (oldestFrame != -1 && mgmtData->frames[oldestFrame].dirty)
	{
		writeBlock(mgmtData->frames[oldestFrame].pageNum, mgmtData->fileHandle, mgmtData->frames[oldestFrame].data);
		mgmtData->numWriteIO++;
	}
	return oldestFrame;
}

static int evictCLOCK(BM_BufferPool *const bm)
{
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	for (int attempts = 0; attempts < 2 * bm->numPages; attempts++)
	{
		int frameIndex = mgmtData->clockHand;
		if (mgmtData->frames[frameIndex].fixCount == 0)
		{
			if (!mgmtData->frames[frameIndex].refBit)
			{
				if (mgmtData->frames[frameIndex].dirty)
				{
					writeBlock(mgmtData->frames[frameIndex].pageNum, mgmtData->fileHandle, mgmtData->frames[frameIndex].data);
					mgmtData->numWriteIO++;
				}
				mgmtData->clockHand = (mgmtData->clockHand + 1) % bm->numPages;
				return frameIndex;
			}
			mgmtData->frames[frameIndex].refBit = false;
		}
		mgmtData->clockHand = (mgmtData->clockHand + 1) % bm->numPages;
	}
	return -1;
}

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
	if (!bm || numPages <= 0)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Invalid buffer pool parameters");
	BM_MgmtData *mgmtData = (BM_MgmtData *)malloc(sizeof(BM_MgmtData));
	if (!mgmtData) THROW(RC_WRITE_FAILED, "Memory allocation failed");
	mgmtData->frames = (BM_PageFrame *)malloc(numPages * sizeof(BM_PageFrame));
	if (!mgmtData->frames) { free(mgmtData); THROW(RC_WRITE_FAILED, "Memory allocation failed"); }
	for (int i = 0; i < numPages; i++)
	{
		mgmtData->frames[i].pageNum = NO_PAGE;
		mgmtData->frames[i].data = (char *)malloc(PAGE_SIZE);
		if (!mgmtData->frames[i].data)
		{
			for (int j = 0; j < i; j++) free(mgmtData->frames[j].data);
			free(mgmtData->frames);
			free(mgmtData);
			THROW(RC_WRITE_FAILED, "Memory allocation failed");
		}
		mgmtData->frames[i].dirty = false;
		mgmtData->frames[i].fixCount = 0;
		mgmtData->frames[i].lastUsed = 0;
		mgmtData->frames[i].accessCount = 0;
		mgmtData->frames[i].refBit = false;
	}
	mgmtData->fileHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
	if (!mgmtData->fileHandle)
	{
		for (int i = 0; i < numPages; i++) free(mgmtData->frames[i].data);
		free(mgmtData->frames);
		free(mgmtData);
		THROW(RC_WRITE_FAILED, "Memory allocation failed");
	}
	RC rc = openPageFile(pageFileName, mgmtData->fileHandle);
	if (rc != RC_OK)
	{
		for (int i = 0; i < numPages; i++) free(mgmtData->frames[i].data);
		free(mgmtData->frames);
		free(mgmtData->fileHandle);
		free(mgmtData);
		return rc;
	}
	mgmtData->numReadIO = 0;
	mgmtData->numWriteIO = 0;
	if (strategy == RS_FIFO)
	{
		mgmtData->fifoQueue = (int *)malloc(numPages * sizeof(int));
		if (!mgmtData->fifoQueue)
		{
			closePageFile(mgmtData->fileHandle);
			for (int i = 0; i < numPages; i++) free(mgmtData->frames[i].data);
			free(mgmtData->frames);
			free(mgmtData->fileHandle);
			free(mgmtData);
			THROW(RC_WRITE_FAILED, "Memory allocation failed");
		}
		for (int i = 0; i < numPages; i++) mgmtData->fifoQueue[i] = i;
		mgmtData->fifoFront = 0;
		mgmtData->fifoRear = 0;
	}
	else mgmtData->fifoQueue = NULL;
	mgmtData->clockHand = 0;
	bm->pageFile = (char *)malloc(strlen(pageFileName) + 1);
	if (!bm->pageFile)
	{
		if (mgmtData->fifoQueue) free(mgmtData->fifoQueue);
		closePageFile(mgmtData->fileHandle);
		for (int i = 0; i < numPages; i++) free(mgmtData->frames[i].data);
		free(mgmtData->frames);
		free(mgmtData->fileHandle);
		free(mgmtData);
		THROW(RC_WRITE_FAILED, "Memory allocation failed");
	}
	strcpy(bm->pageFile, pageFileName);
	bm->numPages = numPages;
	bm->strategy = strategy;
	bm->mgmtData = mgmtData;
	return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	if (!bm || !bm->mgmtData)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Buffer pool is not initialized");
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	forceFlushPool(bm);
	if (mgmtData->fileHandle)
	{
		closePageFile(mgmtData->fileHandle);
		free(mgmtData->fileHandle);
	}
	if (mgmtData->frames)
	{
		for (int i = 0; i < bm->numPages; i++)
			if (mgmtData->frames[i].data) free(mgmtData->frames[i].data);
		free(mgmtData->frames);
	}
	if (mgmtData->fifoQueue) free(mgmtData->fifoQueue);
	free(mgmtData);
	bm->mgmtData = NULL;
	if (bm->pageFile) { free(bm->pageFile); bm->pageFile = NULL; }
	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
	if (!bm || !bm->mgmtData)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Buffer pool is not initialized");
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	for (int i = 0; i < bm->numPages; i++)
	{
		if (mgmtData->frames[i].dirty && mgmtData->frames[i].pageNum != NO_PAGE)
		{
			writeBlock(mgmtData->frames[i].pageNum, mgmtData->fileHandle, mgmtData->frames[i].data);
			mgmtData->frames[i].dirty = false;
			mgmtData->numWriteIO++;
		}
	}
	return RC_OK;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	if (!bm || !bm->mgmtData || !page)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Invalid parameters");
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	static int accessCounter = 0;
	int frameIndex = findFrame(bm, pageNum);
	if (frameIndex != -1)
	{
		mgmtData->frames[frameIndex].fixCount++;
		mgmtData->frames[frameIndex].lastUsed = ++accessCounter;
		mgmtData->frames[frameIndex].accessCount++;
		mgmtData->frames[frameIndex].refBit = true;
		page->pageNum = pageNum;
		page->data = mgmtData->frames[frameIndex].data;
		return RC_OK;
	}
	frameIndex = findFreeFrame(bm);
	if (frameIndex == -1)
	{
		switch (bm->strategy)
		{
		case RS_FIFO: frameIndex = evictFIFO(bm); break;
		case RS_LRU: frameIndex = evictLRU(bm); break;
		case RS_CLOCK: frameIndex = evictCLOCK(bm); break;
		case RS_LFU:
		case RS_LRU_K: frameIndex = evictLRU(bm); break;
		default: frameIndex = evictFIFO(bm); break;
		}
		if (frameIndex == -1)
			THROW(RC_WRITE_FAILED, "Cannot evict page - all frames are pinned");
	}
	readBlock(pageNum, mgmtData->fileHandle, mgmtData->frames[frameIndex].data);
	mgmtData->numReadIO++;
	mgmtData->frames[frameIndex].pageNum = pageNum;
	mgmtData->frames[frameIndex].dirty = false;
	mgmtData->frames[frameIndex].fixCount = 1;
	mgmtData->frames[frameIndex].lastUsed = ++accessCounter;
	mgmtData->frames[frameIndex].accessCount = 1;
	mgmtData->frames[frameIndex].refBit = true;
	if (bm->strategy == RS_FIFO && mgmtData->fifoQueue)
	{
		mgmtData->fifoQueue[mgmtData->fifoRear] = frameIndex;
		mgmtData->fifoRear = (mgmtData->fifoRear + 1) % bm->numPages;
	}
	page->pageNum = pageNum;
	page->data = mgmtData->frames[frameIndex].data;
	return RC_OK;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	if (!bm || !bm->mgmtData || !page)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Invalid parameters");
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	int frameIndex = findFrame(bm, page->pageNum);
	if (frameIndex == -1)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page not found in buffer");
	if (mgmtData->frames[frameIndex].fixCount <= 0)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page fix count is already zero");
	mgmtData->frames[frameIndex].fixCount--;
	return RC_OK;
}

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	if (!bm || !bm->mgmtData || !page)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Invalid parameters");
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	int frameIndex = findFrame(bm, page->pageNum);
	if (frameIndex == -1)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page not found in buffer");
	mgmtData->frames[frameIndex].dirty = true;
	return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	if (!bm || !bm->mgmtData || !page)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Invalid parameters");
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	int frameIndex = findFrame(bm, page->pageNum);
	if (frameIndex == -1)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page not found in buffer");
	writeBlock(page->pageNum, mgmtData->fileHandle, mgmtData->frames[frameIndex].data);
	mgmtData->frames[frameIndex].dirty = false;
	mgmtData->numWriteIO++;
	return RC_OK;
}

PageNumber *getFrameContents(BM_BufferPool *const bm)
{
	if (!bm || !bm->mgmtData) return NULL;
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	PageNumber *contents = (PageNumber *)malloc(bm->numPages * sizeof(PageNumber));
	if (!contents) return NULL;
	for (int i = 0; i < bm->numPages; i++)
		contents[i] = mgmtData->frames[i].pageNum;
	return contents;
}

bool *getDirtyFlags(BM_BufferPool *const bm)
{
	if (!bm || !bm->mgmtData) return NULL;
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	bool *flags = (bool *)malloc(bm->numPages * sizeof(bool));
	if (!flags) return NULL;
	for (int i = 0; i < bm->numPages; i++)
		flags[i] = mgmtData->frames[i].dirty;
	return flags;
}

int *getFixCounts(BM_BufferPool *const bm)
{
	if (!bm || !bm->mgmtData) return NULL;
	BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
	int *counts = (int *)malloc(bm->numPages * sizeof(int));
	if (!counts) return NULL;
	for (int i = 0; i < bm->numPages; i++)
		counts[i] = mgmtData->frames[i].fixCount;
	return counts;
}

int getNumReadIO(BM_BufferPool *const bm)
{
	if (!bm || !bm->mgmtData) return -1;
	return ((BM_MgmtData *)bm->mgmtData)->numReadIO;
}

int getNumWriteIO(BM_BufferPool *const bm)
{
	if (!bm || !bm->mgmtData) return -1;
	return ((BM_MgmtData *)bm->mgmtData)->numWriteIO;
}
