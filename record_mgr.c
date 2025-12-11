#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SLOT_SIZE sizeof(int)
#define PAGE_HEADER_SIZE (sizeof(int) * 3)
#define SCHEMA_PAGE 0
#define FIRST_DATA_PAGE 1

typedef struct TableManager {
	BM_BufferPool *bm;
	int numTuples;
	int firstFreePage;
	Schema *schema;
	int recordSize;
} TableManager;

typedef struct ScanManager {
	int currentPage;
	int currentSlot;
	Expr *condition;
	int totalScanned;
} ScanManager;

static int getRecordSizeHelper(Schema *schema);
static RC writeSchemaToPage(BM_BufferPool *bm, Schema *schema);
static RC readSchemaFromPage(BM_BufferPool *bm, Schema **schema);
static int calculateSlotsPerPage(int recordSize);
static RC findFreeSlot(TableManager *tm, RID *rid);
static RC markSlotAsUsed(BM_BufferPool *bm, RID *rid, int recordSize);
static RC markSlotAsFree(BM_BufferPool *bm, RID *rid, int recordSize);
static bool isSlotUsed(char *page, int slot, int recordSize);
static char* getRecordDataPointer(char *page, int slot, int recordSize);
static int getNextFreePage(char *page);
static void setNextFreePage(char *page, int nextPage);

RC initRecordManager(void *mgmtData) {
	return RC_OK;
}

RC shutdownRecordManager() {
	return RC_OK;
}

RC createTable(char *name, Schema *schema) {
	char fileName[256];
	BM_BufferPool *bm;
	BM_PageHandle *ph;
	RC rc;
	
	sprintf(fileName, "%s.table", name);
	rc = createPageFile(fileName);
	if (rc != RC_OK) return rc;
	
	bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
	rc = initBufferPool(bm, fileName, 3, RS_FIFO, NULL);
	if (rc != RC_OK) {
		free(bm);
		return rc;
	}
	
	rc = writeSchemaToPage(bm, schema);
	if (rc != RC_OK) {
		shutdownBufferPool(bm);
		free(bm);
		return rc;
	}
	
	ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	rc = pinPage(bm, ph, FIRST_DATA_PAGE);
	if (rc != RC_OK) {
		SM_FileHandle fh;
		openPageFile(fileName, &fh);
		appendEmptyBlock(&fh);
		closePageFile(&fh);
		rc = pinPage(bm, ph, FIRST_DATA_PAGE);
		if (rc != RC_OK) {
			shutdownBufferPool(bm);
			free(bm);
			free(ph);
			return rc;
		}
	}
	
	int *header = (int *)ph->data;
	header[0] = calculateSlotsPerPage(getRecordSizeHelper(schema));
	header[1] = header[0];
	header[2] = -1;
	
	markDirty(bm, ph);
	unpinPage(bm, ph);
	shutdownBufferPool(bm);
	free(bm);
	free(ph);
	
	return RC_OK;
}

RC openTable(RM_TableData *rel, char *name) {
	char fileName[256];
	TableManager *tm;
	BM_BufferPool *bm;
	Schema *schema;
	RC rc;
	
	sprintf(fileName, "%s.table", name);
	tm = (TableManager *)malloc(sizeof(TableManager));
	bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
	
	rc = initBufferPool(bm, fileName, 3, RS_FIFO, NULL);
	if (rc != RC_OK) {
		free(tm);
		free(bm);
		return rc;
	}
	
	rc = readSchemaFromPage(bm, &schema);
	if (rc != RC_OK) {
		shutdownBufferPool(bm);
		free(tm);
		free(bm);
		return rc;
	}
	
	tm->bm = bm;
	tm->schema = schema;
	tm->recordSize = getRecordSizeHelper(schema);
	tm->numTuples = 0;
	tm->firstFreePage = FIRST_DATA_PAGE;
	
	BM_PageHandle *ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	int page = FIRST_DATA_PAGE;
	
	while (page >= 0) {
		rc = pinPage(bm, ph, page);
		if (rc != RC_OK) break;
		
		int *header = (int *)ph->data;
		tm->numTuples += (header[0] - header[1]);
		if (header[1] > 0 && tm->firstFreePage == FIRST_DATA_PAGE && page == FIRST_DATA_PAGE)
			tm->firstFreePage = page;
		page = header[2];
		
		unpinPage(bm, ph);
	}
	free(ph);
	
	rel->name = (char *)malloc(strlen(name) + 1);
	if (!rel->name) {
		freeSchema(schema);
		shutdownBufferPool(bm);
		free(tm);
		free(bm);
		THROW(RC_WRITE_FAILED, "Memory allocation failed");
	}
	strcpy(rel->name, name);
	rel->schema = schema;
	rel->mgmtData = tm;
	
	return RC_OK;
}

RC closeTable(RM_TableData *rel) {
	if (!rel || !rel->mgmtData) THROW(RC_FILE_HANDLE_NOT_INIT, "Table not initialized");
	TableManager *tm = (TableManager *)rel->mgmtData;
	forceFlushPool(tm->bm);
	shutdownBufferPool(tm->bm);
	free(tm->bm);
	freeSchema(rel->schema);
	free(tm);
	if (rel->name) { free(rel->name); rel->name = NULL; }
	rel->mgmtData = NULL;
	return RC_OK;
}

RC deleteTable(char *name) {
	char fileName[256];
	sprintf(fileName, "%s.table", name);
	return destroyPageFile(fileName);
}

int getNumTuples(RM_TableData *rel) {
	if (!rel || !rel->mgmtData) return 0;
	return ((TableManager *)rel->mgmtData)->numTuples;
}

RC insertRecord(RM_TableData *rel, Record *record) {
	TableManager *tm = (TableManager *)rel->mgmtData;
	BM_PageHandle *ph;
	RID rid;
	RC rc;
	char *recordPos;
	
	rc = findFreeSlot(tm, &rid);
	if (rc != RC_OK) return rc;
	
	ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	rc = pinPage(tm->bm, ph, rid.page);
	if (rc != RC_OK) {
		free(ph);
		return rc;
	}
	
	recordPos = getRecordDataPointer(ph->data, rid.slot, tm->recordSize);
	memcpy(recordPos, record->data, tm->recordSize);
	markSlotAsUsed(tm->bm, &rid, tm->recordSize);
	record->id = rid;
	markDirty(tm->bm, ph);
	unpinPage(tm->bm, ph);
	free(ph);
	tm->numTuples++;
	return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id) {
	TableManager *tm = (TableManager *)rel->mgmtData;
	BM_PageHandle *ph;
	RC rc;
	
	ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	rc = pinPage(tm->bm, ph, id.page);
	if (rc != RC_OK) {
		free(ph);
		return rc;
	}
	
	if (!isSlotUsed(ph->data, id.slot, tm->recordSize)) {
		unpinPage(tm->bm, ph);
		free(ph);
		THROW(RC_FILE_NOT_FOUND, "Record not found");
	}
	
	markSlotAsFree(tm->bm, &id, tm->recordSize);
	if (getNextFreePage(ph->data) == -1) {
		BM_PageHandle *infoPh = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
		rc = pinPage(tm->bm, infoPh, SCHEMA_PAGE);
		if (rc == RC_OK) {
			int *infoHeader = (int *)infoPh->data;
			setNextFreePage(ph->data, tm->firstFreePage);
			tm->firstFreePage = id.page;
			markDirty(tm->bm, infoPh);
			unpinPage(tm->bm, infoPh);
		}
		free(infoPh);
	}
	
	markDirty(tm->bm, ph);
	unpinPage(tm->bm, ph);
	free(ph);
	tm->numTuples--;
	return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {
	TableManager *tm = (TableManager *)rel->mgmtData;
	BM_PageHandle *ph;
	RC rc;
	char *recordPos;
	
	ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	rc = pinPage(tm->bm, ph, record->id.page);
	if (rc != RC_OK) {
		free(ph);
		return rc;
	}
	
	if (!isSlotUsed(ph->data, record->id.slot, tm->recordSize)) {
		unpinPage(tm->bm, ph);
		free(ph);
		THROW(RC_FILE_NOT_FOUND, "Record not found");
	}
	
	recordPos = getRecordDataPointer(ph->data, record->id.slot, tm->recordSize);
	memcpy(recordPos, record->data, tm->recordSize);
	markDirty(tm->bm, ph);
	unpinPage(tm->bm, ph);
	free(ph);
	return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
	TableManager *tm = (TableManager *)rel->mgmtData;
	BM_PageHandle *ph;
	RC rc;
	char *recordPos;
	
	ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	rc = pinPage(tm->bm, ph, id.page);
	if (rc != RC_OK) {
		free(ph);
		return rc;
	}
	
	if (!isSlotUsed(ph->data, id.slot, tm->recordSize)) {
		unpinPage(tm->bm, ph);
		free(ph);
		THROW(RC_FILE_NOT_FOUND, "Record not found");
	}
	
	recordPos = getRecordDataPointer(ph->data, id.slot, tm->recordSize);
	record->id = id;
	if (!record->data) record->data = (char *)malloc(tm->recordSize);
	memcpy(record->data, recordPos, tm->recordSize);
	unpinPage(tm->bm, ph);
	free(ph);
	return RC_OK;
}

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
	ScanManager *sm = (ScanManager *)malloc(sizeof(ScanManager));
	sm->currentPage = FIRST_DATA_PAGE;
	sm->currentSlot = 0;
	sm->condition = cond;
	sm->totalScanned = 0;
	scan->rel = rel;
	scan->mgmtData = sm;
	return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record) {
	ScanManager *sm = (ScanManager *)scan->mgmtData;
	TableManager *tm = (TableManager *)scan->rel->mgmtData;
	BM_PageHandle *ph;
	RC rc;
	Value *result;
	int slotsPerPage = calculateSlotsPerPage(tm->recordSize);
	
	ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	
	while (sm->currentPage >= 0) {
		rc = pinPage(tm->bm, ph, sm->currentPage);
		if (rc != RC_OK) {
			free(ph);
			THROW(RC_RM_NO_MORE_TUPLES, "No more tuples");
		}
		
		int *header = (int *)ph->data;
		int numSlots = header[0];
		
		while (sm->currentSlot < numSlots) {
			if (isSlotUsed(ph->data, sm->currentSlot, tm->recordSize)) {
				RID rid = {sm->currentPage, sm->currentSlot};
				char *recordPos = getRecordDataPointer(ph->data, sm->currentSlot, tm->recordSize);
				record->id = rid;
				if (!record->data) record->data = (char *)malloc(tm->recordSize);
				memcpy(record->data, recordPos, tm->recordSize);
				
				bool matches = true;
				if (sm->condition) {
					rc = evalExpr(record, scan->rel->schema, sm->condition, &result);
					if (rc != RC_OK) {
						unpinPage(tm->bm, ph);
						free(ph);
						return rc;
					}
					if (result && result->dt == DT_BOOL) {
						matches = result->v.boolV;
						freeVal(result);
					}
				}
				
				sm->currentSlot++;
				if (matches) {
					unpinPage(tm->bm, ph);
					free(ph);
					return RC_OK;
				}
			} else {
				sm->currentSlot++;
			}
		}
		
		sm->currentPage = header[2];
		sm->currentSlot = 0;
		unpinPage(tm->bm, ph);
	}
	
	free(ph);
	THROW(RC_RM_NO_MORE_TUPLES, "No more tuples");
}

RC closeScan(RM_ScanHandle *scan) {
	if (!scan || !scan->mgmtData) THROW(RC_FILE_HANDLE_NOT_INIT, "Scan not initialized");
	free(scan->mgmtData);
	scan->mgmtData = NULL;
	return RC_OK;
}

int getRecordSize(Schema *schema) {
	return getRecordSizeHelper(schema);
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
	Schema *schema = (Schema *)malloc(sizeof(Schema));
	if (!schema) return NULL;
	
	schema->numAttr = numAttr;
	schema->attrNames = (char **)malloc(numAttr * sizeof(char *));
	schema->dataTypes = (DataType *)malloc(numAttr * sizeof(DataType));
	schema->typeLength = (int *)malloc(numAttr * sizeof(int));
	schema->keySize = keySize;
	schema->keyAttrs = (int *)malloc(keySize * sizeof(int));
	
	if (!schema->attrNames || !schema->dataTypes || !schema->typeLength || !schema->keyAttrs) {
		if (schema->attrNames) free(schema->attrNames);
		if (schema->dataTypes) free(schema->dataTypes);
		if (schema->typeLength) free(schema->typeLength);
		if (schema->keyAttrs) free(schema->keyAttrs);
		free(schema);
		return NULL;
	}
	
	for (int i = 0; i < numAttr; i++) {
		schema->attrNames[i] = (char *)malloc(strlen(attrNames[i]) + 1);
		strcpy(schema->attrNames[i], attrNames[i]);
		schema->dataTypes[i] = dataTypes[i];
		schema->typeLength[i] = typeLength[i];
	}
	
	for (int i = 0; i < keySize; i++)
		schema->keyAttrs[i] = keys[i];
	
	return schema;
}

RC freeSchema(Schema *schema) {
	if (!schema) return RC_OK;
	if (schema->attrNames) {
		for (int i = 0; i < schema->numAttr; i++)
			if (schema->attrNames[i]) free(schema->attrNames[i]);
		free(schema->attrNames);
	}
	if (schema->dataTypes) free(schema->dataTypes);
	if (schema->typeLength) free(schema->typeLength);
	if (schema->keyAttrs) free(schema->keyAttrs);
	free(schema);
	return RC_OK;
}

RC createRecord(Record **record, Schema *schema) {
	if (!record || !schema) THROW(RC_FILE_HANDLE_NOT_INIT, "Invalid parameters");
	Record *newRecord = (Record *)malloc(sizeof(Record));
	if (!newRecord) THROW(RC_WRITE_FAILED, "Memory allocation failed");
	int recordSize = getRecordSizeHelper(schema);
	newRecord->data = (char *)calloc(recordSize, sizeof(char));
	if (!newRecord->data) {
		free(newRecord);
		THROW(RC_WRITE_FAILED, "Memory allocation failed");
	}
	newRecord->id.page = -1;
	newRecord->id.slot = -1;
	*record = newRecord;
	return RC_OK;
}

RC freeRecord(Record *record) {
	if (!record) return RC_OK;
	if (record->data) free(record->data);
	free(record);
	return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
	if (!record || !schema || !value || attrNum < 0 || attrNum >= schema->numAttr)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Invalid parameters");
	int offset = 0;
	for (int i = 0; i < attrNum; i++) {
		switch (schema->dataTypes[i]) {
		case DT_INT: offset += sizeof(int); break;
		case DT_FLOAT: offset += sizeof(float); break;
		case DT_BOOL: offset += sizeof(bool); break;
		case DT_STRING: offset += schema->typeLength[i]; break;
		}
	}
	Value *val = (Value *)malloc(sizeof(Value));
	if (!val) THROW(RC_WRITE_FAILED, "Memory allocation failed");
	val->dt = schema->dataTypes[attrNum];
	char *attrData = record->data + offset;
	switch (schema->dataTypes[attrNum]) {
	case DT_INT: memcpy(&(val->v.intV), attrData, sizeof(int)); break;
	case DT_FLOAT: memcpy(&(val->v.floatV), attrData, sizeof(float)); break;
	case DT_BOOL: memcpy(&(val->v.boolV), attrData, sizeof(bool)); break;
	case DT_STRING:
		val->v.stringV = (char *)malloc(schema->typeLength[attrNum] + 1);
		if (!val->v.stringV) { free(val); THROW(RC_WRITE_FAILED, "Memory allocation failed"); }
		memcpy(val->v.stringV, attrData, schema->typeLength[attrNum]);
		val->v.stringV[schema->typeLength[attrNum]] = '\0';
		break;
	}
	*value = val;
	return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
	if (!record || !schema || !value || attrNum < 0 || attrNum >= schema->numAttr)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Invalid parameters");
	if (value->dt != schema->dataTypes[attrNum])
		THROW(RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE, "Data type mismatch");
	int offset = 0;
	for (int i = 0; i < attrNum; i++) {
		switch (schema->dataTypes[i]) {
		case DT_INT: offset += sizeof(int); break;
		case DT_FLOAT: offset += sizeof(float); break;
		case DT_BOOL: offset += sizeof(bool); break;
		case DT_STRING: offset += schema->typeLength[i]; break;
		}
	}
	char *attrData = record->data + offset;
	switch (schema->dataTypes[attrNum]) {
	case DT_INT: memcpy(attrData, &(value->v.intV), sizeof(int)); break;
	case DT_FLOAT: memcpy(attrData, &(value->v.floatV), sizeof(float)); break;
	case DT_BOOL: memcpy(attrData, &(value->v.boolV), sizeof(bool)); break;
	case DT_STRING: memcpy(attrData, value->v.stringV, schema->typeLength[attrNum]); break;
	}
	return RC_OK;
}

static int getRecordSizeHelper(Schema *schema) {
	int size = 0;
	for (int i = 0; i < schema->numAttr; i++) {
		switch (schema->dataTypes[i]) {
		case DT_INT: size += sizeof(int); break;
		case DT_FLOAT: size += sizeof(float); break;
		case DT_BOOL: size += sizeof(bool); break;
		case DT_STRING: size += schema->typeLength[i]; break;
		}
	}
	return size;
}

static int calculateSlotsPerPage(int recordSize) {
	int slotOverhead = 1;
	int usableSpace = PAGE_SIZE - PAGE_HEADER_SIZE;
	return usableSpace / (recordSize + slotOverhead);
}

static RC writeSchemaToPage(BM_BufferPool *bm, Schema *schema) {
	BM_PageHandle *ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	RC rc = pinPage(bm, ph, SCHEMA_PAGE);
	if (rc != RC_OK) {
		free(ph);
		return rc;
	}
	
	char *data = ph->data;
	int offset = 0;
	
	memcpy(data + offset, &(schema->numAttr), sizeof(int));
	offset += sizeof(int);
	memcpy(data + offset, &(schema->keySize), sizeof(int));
	offset += sizeof(int);
	
	for (int i = 0; i < schema->numAttr; i++) {
		memcpy(data + offset, &(schema->dataTypes[i]), sizeof(DataType));
		offset += sizeof(DataType);
		memcpy(data + offset, &(schema->typeLength[i]), sizeof(int));
		offset += sizeof(int);
		int nameLen = strlen(schema->attrNames[i]);
		if (offset + sizeof(int) + nameLen > PAGE_SIZE) {
			unpinPage(bm, ph);
			free(ph);
			THROW(RC_WRITE_FAILED, "Schema too large for page");
		}
		memcpy(data + offset, &nameLen, sizeof(int));
		offset += sizeof(int);
		memcpy(data + offset, schema->attrNames[i], nameLen);
		offset += nameLen;
	}
	
	for (int i = 0; i < schema->keySize; i++) {
		if (offset + sizeof(int) > PAGE_SIZE) {
			unpinPage(bm, ph);
			free(ph);
			THROW(RC_WRITE_FAILED, "Schema too large for page");
		}
		memcpy(data + offset, &(schema->keyAttrs[i]), sizeof(int));
		offset += sizeof(int);
	}
	
	markDirty(bm, ph);
	unpinPage(bm, ph);
	free(ph);
	return RC_OK;
}

static RC readSchemaFromPage(BM_BufferPool *bm, Schema **schema) {
	BM_PageHandle *ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	RC rc = pinPage(bm, ph, SCHEMA_PAGE);
	if (rc != RC_OK) {
		free(ph);
		return rc;
	}
	
	Schema *newSchema = (Schema *)malloc(sizeof(Schema));
	char *data = ph->data;
	int offset = 0;
	
	memcpy(&(newSchema->numAttr), data + offset, sizeof(int));
	offset += sizeof(int);
	memcpy(&(newSchema->keySize), data + offset, sizeof(int));
	offset += sizeof(int);
	
	newSchema->attrNames = (char **)malloc(newSchema->numAttr * sizeof(char *));
	newSchema->dataTypes = (DataType *)malloc(newSchema->numAttr * sizeof(DataType));
	newSchema->typeLength = (int *)malloc(newSchema->numAttr * sizeof(int));
	
	if (!newSchema->attrNames || !newSchema->dataTypes || !newSchema->typeLength) {
		if (newSchema->attrNames) free(newSchema->attrNames);
		if (newSchema->dataTypes) free(newSchema->dataTypes);
		if (newSchema->typeLength) free(newSchema->typeLength);
		unpinPage(bm, ph);
		free(ph);
		free(newSchema);
		THROW(RC_WRITE_FAILED, "Memory allocation failed");
	}
	
	for (int i = 0; i < newSchema->numAttr; i++) {
		memcpy(&(newSchema->dataTypes[i]), data + offset, sizeof(DataType));
		offset += sizeof(DataType);
		memcpy(&(newSchema->typeLength[i]), data + offset, sizeof(int));
		offset += sizeof(int);
		int nameLen;
		memcpy(&nameLen, data + offset, sizeof(int));
		offset += sizeof(int);
		newSchema->attrNames[i] = (char *)malloc(nameLen + 1);
		if (!newSchema->attrNames[i]) {
			for (int j = 0; j < i; j++) free(newSchema->attrNames[j]);
			free(newSchema->attrNames);
			free(newSchema->dataTypes);
			free(newSchema->typeLength);
			unpinPage(bm, ph);
			free(ph);
			free(newSchema);
			THROW(RC_WRITE_FAILED, "Memory allocation failed");
		}
		memcpy(newSchema->attrNames[i], data + offset, nameLen);
		newSchema->attrNames[i][nameLen] = '\0';
		offset += nameLen;
	}
	
	newSchema->keyAttrs = (int *)malloc(newSchema->keySize * sizeof(int));
	if (!newSchema->keyAttrs) {
		for (int i = 0; i < newSchema->numAttr; i++) free(newSchema->attrNames[i]);
		free(newSchema->attrNames);
		free(newSchema->dataTypes);
		free(newSchema->typeLength);
		unpinPage(bm, ph);
		free(ph);
		free(newSchema);
		THROW(RC_WRITE_FAILED, "Memory allocation failed");
	}
	
	for (int i = 0; i < newSchema->keySize; i++) {
		memcpy(&(newSchema->keyAttrs[i]), data + offset, sizeof(int));
		offset += sizeof(int);
	}
	
	unpinPage(bm, ph);
	free(ph);
	*schema = newSchema;
	return RC_OK;
}

static RC findFreeSlot(TableManager *tm, RID *rid) {
	BM_PageHandle *ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	int page = tm->firstFreePage;
	RC rc;
	
	while (page >= 0) {
		rc = pinPage(tm->bm, ph, page);
		if (rc != RC_OK) break;
		
		int *header = (int *)ph->data;
		if (header[1] > 0) {
			for (int slot = 0; slot < header[0]; slot++) {
				if (!isSlotUsed(ph->data, slot, tm->recordSize)) {
					rid->page = page;
					rid->slot = slot;
					unpinPage(tm->bm, ph);
					free(ph);
					return RC_OK;
				}
			}
		}
		page = header[2];
		unpinPage(tm->bm, ph);
	}
	
	SM_FileHandle fh;
	rc = openPageFile(tm->bm->pageFile, &fh);
	if (rc != RC_OK) {
		free(ph);
		return rc;
	}
	
	int newPage = fh.totalNumPages;
	appendEmptyBlock(&fh);
	closePageFile(&fh);
	
	rc = pinPage(tm->bm, ph, newPage);
	if (rc != RC_OK) {
		free(ph);
		return rc;
	}
	
	int *header = (int *)ph->data;
	header[0] = calculateSlotsPerPage(tm->recordSize);
	header[1] = header[0];
	header[2] = -1;
	
	if (tm->firstFreePage == FIRST_DATA_PAGE) {
		BM_PageHandle *firstPh = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
		rc = pinPage(tm->bm, firstPh, FIRST_DATA_PAGE);
		if (rc == RC_OK) {
			int *firstHeader = (int *)firstPh->data;
			header[2] = firstHeader[2];
			firstHeader[2] = newPage;
			markDirty(tm->bm, firstPh);
			unpinPage(tm->bm, firstPh);
		}
		free(firstPh);
	}
	
	rid->page = newPage;
	rid->slot = 0;
	tm->firstFreePage = newPage;
	
	markDirty(tm->bm, ph);
	unpinPage(tm->bm, ph);
	free(ph);
	return RC_OK;
}

static RC markSlotAsUsed(BM_BufferPool *bm, RID *rid, int recordSize) {
	BM_PageHandle *ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	RC rc = pinPage(bm, ph, rid->page);
	if (rc != RC_OK) {
		free(ph);
		return rc;
	}
	
	int *header = (int *)ph->data;
	char *tombstone = ph->data + PAGE_HEADER_SIZE + rid->slot * (recordSize + 1);
	*tombstone = 1;
	header[1]--;
	
	markDirty(bm, ph);
	unpinPage(bm, ph);
	free(ph);
	return RC_OK;
}

static RC markSlotAsFree(BM_BufferPool *bm, RID *rid, int recordSize) {
	BM_PageHandle *ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	RC rc = pinPage(bm, ph, rid->page);
	if (rc != RC_OK) {
		free(ph);
		return rc;
	}
	
	int *header = (int *)ph->data;
	char *tombstone = ph->data + PAGE_HEADER_SIZE + rid->slot * (recordSize + 1);
	*tombstone = 0;
	header[1]++;
	
	markDirty(bm, ph);
	unpinPage(bm, ph);
	free(ph);
	return RC_OK;
}

static bool isSlotUsed(char *page, int slot, int recordSize) {
	char *tombstone = page + PAGE_HEADER_SIZE + slot * (recordSize + 1);
	return (*tombstone == 1);
}

static char* getRecordDataPointer(char *page, int slot, int recordSize) {
	return page + PAGE_HEADER_SIZE + slot * (recordSize + 1) + 1;
}

static int getNextFreePage(char *page) {
	return ((int *)page)[2];
}

static void setNextFreePage(char *page, int nextPage) {
	((int *)page)[2] = nextPage;
}
