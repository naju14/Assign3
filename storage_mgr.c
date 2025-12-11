#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

void initStorageManager(void) {}

RC createPageFile(char *fileName)
{
	FILE *file = fopen(fileName, "w+b");
	if (!file) THROW(RC_FILE_NOT_FOUND, "Cannot create page file");
	char page[PAGE_SIZE];
	memset(page, 0, PAGE_SIZE);
	if (fwrite(page, sizeof(char), PAGE_SIZE, file) != PAGE_SIZE)
	{
		fclose(file);
		THROW(RC_WRITE_FAILED, "Failed to write first page");
	}
	fclose(file);
	return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
	if (!fHandle) THROW(RC_FILE_HANDLE_NOT_INIT, "File handle is NULL");
	FILE *file = fopen(fileName, "r+b");
	if (!file) THROW(RC_FILE_NOT_FOUND, "Cannot open page file");
	struct stat fileStat;
	if (fstat(fileno(file), &fileStat) != 0)
	{
		fclose(file);
		THROW(RC_FILE_NOT_FOUND, "Cannot get file size");
	}
	fHandle->fileName = (char *)malloc(strlen(fileName) + 1);
	if (!fHandle->fileName)
	{
		fclose(file);
		THROW(RC_WRITE_FAILED, "Memory allocation failed");
	}
	strcpy(fHandle->fileName, fileName);
	fHandle->totalNumPages = fileStat.st_size / PAGE_SIZE;
	fHandle->curPagePos = 0;
	fHandle->mgmtInfo = file;
	return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle)
{
	if (!fHandle || !fHandle->mgmtInfo)
		THROW(RC_FILE_HANDLE_NOT_INIT, "File handle is not initialized");
	fclose((FILE *)fHandle->mgmtInfo);
	if (fHandle->fileName) free(fHandle->fileName);
	fHandle->fileName = NULL;
	fHandle->mgmtInfo = NULL;
	fHandle->totalNumPages = 0;
	fHandle->curPagePos = 0;
	return RC_OK;
}

RC destroyPageFile(char *fileName)
{
	if (remove(fileName) != 0)
		THROW(RC_FILE_NOT_FOUND, "Cannot delete page file");
	return RC_OK;
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (!fHandle || !fHandle->mgmtInfo)
		THROW(RC_FILE_HANDLE_NOT_INIT, "File handle is not initialized");
	if (pageNum < 0 || pageNum >= fHandle->totalNumPages)
		THROW(RC_READ_NON_EXISTING_PAGE, "Page number out of range");
	if (!memPage) THROW(RC_FILE_HANDLE_NOT_INIT, "Memory page is NULL");
	FILE *file = (FILE *)fHandle->mgmtInfo;
	long offset = (long)pageNum * PAGE_SIZE;
	if (fseek(file, offset, SEEK_SET) != 0)
		THROW(RC_READ_NON_EXISTING_PAGE, "Cannot seek to page");
	if (fread(memPage, sizeof(char), PAGE_SIZE, file) != PAGE_SIZE)
		THROW(RC_READ_NON_EXISTING_PAGE, "Cannot read page");
	fHandle->curPagePos = pageNum;
	return RC_OK;
}

int getBlockPos(SM_FileHandle *fHandle)
{
	return fHandle ? fHandle->curPagePos : -1;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (!fHandle) THROW(RC_FILE_HANDLE_NOT_INIT, "File handle is not initialized");
	if (fHandle->curPagePos <= 0)
		THROW(RC_READ_NON_EXISTING_PAGE, "No previous page");
	return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (!fHandle || !fHandle->mgmtInfo)
		THROW(RC_FILE_HANDLE_NOT_INIT, "File handle is not initialized");
	return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (!fHandle) THROW(RC_FILE_HANDLE_NOT_INIT, "File handle is not initialized");
	if (fHandle->curPagePos >= fHandle->totalNumPages - 1)
		THROW(RC_READ_NON_EXISTING_PAGE, "No next page");
	return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (!fHandle) THROW(RC_FILE_HANDLE_NOT_INIT, "File handle is not initialized");
	if (fHandle->totalNumPages == 0)
		THROW(RC_READ_NON_EXISTING_PAGE, "File is empty");
	return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (!fHandle || !fHandle->mgmtInfo)
		THROW(RC_FILE_HANDLE_NOT_INIT, "File handle is not initialized");
	if (pageNum < 0 || pageNum >= fHandle->totalNumPages)
		THROW(RC_WRITE_FAILED, "Page number out of range");
	if (!memPage) THROW(RC_FILE_HANDLE_NOT_INIT, "Memory page is NULL");
	FILE *file = (FILE *)fHandle->mgmtInfo;
	long offset = (long)pageNum * PAGE_SIZE;
	if (fseek(file, offset, SEEK_SET) != 0)
		THROW(RC_WRITE_FAILED, "Cannot seek to page");
	if (fwrite(memPage, sizeof(char), PAGE_SIZE, file) != PAGE_SIZE)
		THROW(RC_WRITE_FAILED, "Cannot write page");
	fflush(file);
	fHandle->curPagePos = pageNum;
	return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (!fHandle || !fHandle->mgmtInfo)
		THROW(RC_FILE_HANDLE_NOT_INIT, "File handle is not initialized");
	return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock(SM_FileHandle *fHandle)
{
	if (!fHandle || !fHandle->mgmtInfo)
		THROW(RC_FILE_HANDLE_NOT_INIT, "File handle is not initialized");
	FILE *file = (FILE *)fHandle->mgmtInfo;
	char page[PAGE_SIZE];
	memset(page, 0, PAGE_SIZE);
	long offset = (long)fHandle->totalNumPages * PAGE_SIZE;
	if (fseek(file, offset, SEEK_SET) != 0)
		THROW(RC_WRITE_FAILED, "Cannot seek to end of file");
	if (fwrite(page, sizeof(char), PAGE_SIZE, file) != PAGE_SIZE)
		THROW(RC_WRITE_FAILED, "Cannot append page");
	fflush(file);
	fHandle->totalNumPages++;
	fHandle->curPagePos = fHandle->totalNumPages - 1;
	return RC_OK;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
	if (!fHandle || !fHandle->mgmtInfo)
		THROW(RC_FILE_HANDLE_NOT_INIT, "File handle is not initialized");
	while (fHandle->totalNumPages < numberOfPages)
	{
		RC rc = appendEmptyBlock(fHandle);
		if (rc != RC_OK) return rc;
	}
	return RC_OK;
}
