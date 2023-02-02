//
// Created by Qiming lyu on 2022/11/1.
//
// table and manager

#include <stdlib.h>
#include "record_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "string.h"

// maximum length of an attribute
#define ATTR_NAME_SIZE 16
// How many slots have been used in a page
#define HEADER_SIZE 512
// Schema info
#define SCHEMA_PAGE 0
// table 数据存放的page页
#define TABLE_PAGE 1
// data page
#define RECODER_START_PAGE 2

typedef struct TableInfo
{
    int numTuples;
    int LastPageNum;
    int lastSlot;
    int recordSize;
    // Active slot
    int maxSlot;
    BM_BufferPool *bm;
} TableInfo;

typedef struct ScanManager
{
    RID rid;
    Expr *cond;
    int scannedTuples;
} ScanManager;

SM_PageHandle schemaToPageHandle(Schema *pSchema);

SM_PageHandle tableInfoToPageHandle(TableInfo *pInfo);

Schema *pageHandleToSchema(BM_PageHandle *pHandle);

void pageHandleToTableInfo(BM_PageHandle *pHandle, TableInfo *pInfo);

void getAttrOffset(const Schema *schema, int attrNum, int *offset);

RC writeRecord(Schema *schema, Record *record, BM_PageHandle *pageHandle);

RC updateInfo(TableInfo *info, BM_PageHandle *pageHandle);

extern RC initRecordManager (void *mgmtData){
    initStorageManager();
    return RC_OK;
}

extern RC shutdownRecordManager (){
    return RC_OK;
}

extern RC createTable (char *name, Schema *schema){
    RC result;
    if ((result = createPageFile(name)) != RC_OK) {
        printf("error createPageFile....\n");
        return result;
    }

    SM_FileHandle fileHandle;
    if ((result = openPageFile(name, &fileHandle)) != RC_OK) {
        printf("error openPageFile....\n");
        return result;
    }

    SM_PageHandle pageHandle = schemaToPageHandle(schema);
    if ((result = writeBlock(SCHEMA_PAGE, &fileHandle, pageHandle)) != RC_OK) {
        printf("error writeBlock 0....\n");
        free(pageHandle);
        return result;
    }
    free(pageHandle);

    TableInfo tableInfo;
    tableInfo.LastPageNum = RECODER_START_PAGE;
    tableInfo.lastSlot = -1;
    tableInfo.recordSize = getRecordSize(schema);
    tableInfo.numTuples = 0;
    tableInfo.maxSlot = (PAGE_SIZE - HEADER_SIZE) / tableInfo.recordSize;
    pageHandle = tableInfoToPageHandle(&tableInfo);
    if ((result = writeBlock(TABLE_PAGE, &fileHandle, pageHandle)) != RC_OK) {
        printf("error writeBlock 1....\n");
        free(pageHandle);
        return result;
    }
    free(pageHandle);

    if ((result = closePageFile(&fileHandle)) != RC_OK) {
        printf("error closePageFile....\n");
        return result;
    }

    return RC_OK;
}

extern RC openTable (RM_TableData *rel, char *name){
    BM_BufferPool *bm = malloc(sizeof(BM_BufferPool));
    RC result;
    if ((result = initBufferPool(bm, name, 10, RS_FIFO, NULL)) != RC_OK) {
        printf("error openTable initBufferPool....\n");
        return result;
    }

    BM_PageHandle pageHandle;
    if ((result = pinPage(bm, &pageHandle, SCHEMA_PAGE)) != RC_OK) {
        printf("error openTable pinPage 0....\n");
        return result;
    }

    rel->schema = pageHandleToSchema(&pageHandle);

    rel->name = calloc(1, strlen(name)*sizeof(char) + 1);
    strcpy(rel->name, name);

    if ((result = pinPage(bm, &pageHandle, TABLE_PAGE)) != RC_OK) {
        printf("error openTable pinPage 1....\n");
        return result;
    }
    TableInfo *tableInfo = calloc(1, sizeof(TableInfo));
    pageHandleToTableInfo(&pageHandle, tableInfo);
    tableInfo->bm = bm;
    rel->mgmtData = tableInfo;

    return RC_OK;
}

extern RC closeTable (RM_TableData *rel){
    TableInfo *info = rel->mgmtData;
    shutdownBufferPool(info->bm);
    free(info->bm);
    free(rel->name);
    free(rel->mgmtData);
    freeSchema(rel->schema);

    return RC_OK;
}

extern RC deleteTable (char *name){
    RC result;
    if ((result = destroyPageFile(name)) != RC_OK) {
        return result;
    }

    return RC_OK;
}
extern int getNumTuples (RM_TableData *rel){
    TableInfo *info = rel->mgmtData;
    return info->numTuples;
}

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record){
    TableInfo *info = rel->mgmtData;
    RC result;
    int curSize = (info->lastSlot + 1) * info->recordSize;
    BM_PageHandle *pageHandle = calloc(1, sizeof(BM_PageHandle));
    if (curSize + info->recordSize < (PAGE_SIZE - HEADER_SIZE)) {
        info->lastSlot++;
        record->id.page = info->LastPageNum;
        record->id.slot = info->lastSlot;
        if ((result = pinPage(info->bm, pageHandle, record->id.page)) != RC_OK ) {
            return result;
        }
        if ((result = writeRecord(rel->schema, record, pageHandle)) != RC_OK) {
            return result;
        }
    } else {
        info->LastPageNum++;
        info->lastSlot = 0;
        record->id.page = info->LastPageNum;
        record->id.slot = info->lastSlot;

        if ((result = pinPage(info->bm, pageHandle, record->id.page)) != RC_OK ) {
            return result;
        }
        if ((result = writeRecord(rel->schema, record, pageHandle)) != RC_OK) {
            return result;
        }
    }

    pageHandle->data[record->id.slot] = 1;
    markDirty(info->bm, pageHandle);
    unpinPage(info->bm, pageHandle);
    info->numTuples++;
    updateInfo(info, pageHandle);
    free(pageHandle);
    return RC_OK;
}

extern RC deleteRecord (RM_TableData *rel, RID id){
    BM_PageHandle *pageHandle = calloc(1, sizeof(BM_PageHandle));
    RC result;
    TableInfo *info = rel->mgmtData;
    if ((result = pinPage(info->bm, pageHandle, id.page)) != RC_OK){
        return result;
    }
    if (!pageHandle->data[id.slot]) {
        return RC_RM_NOT_FOUND_TUPLES;
    }

    int offset = (id.slot) * info->recordSize;
    char *data = pageHandle->data + HEADER_SIZE + offset;
    memset(data, 0, getRecordSize(rel->schema));
    pageHandle->data[id.slot] = 0;
    markDirty(info->bm, pageHandle);
    unpinPage(info->bm, pageHandle);

    info->numTuples--;
    updateInfo(info, pageHandle);
    free(pageHandle);
    return RC_OK;
}

extern RC updateRecord (RM_TableData *rel, Record *record){
    TableInfo *info = rel->mgmtData;
    BM_PageHandle *pageHandle = calloc(1, sizeof(BM_PageHandle));
    RC result;
    if ((result = pinPage(info->bm, pageHandle, record->id.page)) != RC_OK){
        return result;
    }
    if (!pageHandle->data[record->id.slot]) {
        return RC_RM_NOT_FOUND_TUPLES;
    }

    int offset = (record->id.slot) * info->recordSize;
    char *data = pageHandle->data + HEADER_SIZE + offset;
    memset(data, 0, getRecordSize(rel->schema));
    if ((result = writeRecord(rel->schema, record, pageHandle)) != RC_OK) {
        return result;
    }

    markDirty(info->bm, pageHandle);
    unpinPage(info->bm, pageHandle);
    free(pageHandle);
    return RC_OK;
}
extern RC getRecord (RM_TableData *rel, RID id, Record *record){
    record->id.page = id.page;
    record->id.slot = id.slot;

    TableInfo *info = rel->mgmtData;
    BM_PageHandle *pageHandle = calloc(1, sizeof(BM_PageHandle));
    RC result;
    if ((result = pinPage(info->bm, pageHandle, id.page)) != RC_OK) {
        free(pageHandle);
        return result;
    }
    if (!pageHandle->data[record->id.slot]) {
        free(pageHandle);
        return RC_RM_NO_MORE_TUPLES;
    }

    int offset = (id.slot) * info->recordSize;
    char *data = pageHandle->data + HEADER_SIZE + offset;
    Value *value;
    for (int i = 0; i < rel->schema->numAttr; ++i) {
        value = malloc(sizeof(Value));
        memset(value, 0, sizeof(Value));
        value->dt = rel->schema->dataTypes[i];
        switch (value->dt) {
            case DT_STRING:
                value->v.stringV = malloc(sizeof(char) * rel->schema->typeLength[i]);
                memset(value->v.stringV, 0, sizeof(char) * rel->schema->typeLength[i]);
                strncpy(value->v.stringV, data, rel->schema->typeLength[i]);
                data += (rel->schema->typeLength[i] +1);
                break;
            case DT_BOOL:
                value->v.boolV = *(bool *)data;
                data += sizeof(bool);
                break;
            case DT_FLOAT:
                value->v.floatV = *(float *)data;
                data += sizeof(float);
                break;
            case DT_INT:
                value->v.intV = *(int *)data;
                data += sizeof(int);
                break;
            default:
                freeVal(value);
                free(pageHandle);
                return RC_RM_UNKOWN_DATATYPE;
        }

        if ((result = setAttr(record, rel->schema, i,value)) != RC_OK) {
            freeVal(value);
            free(pageHandle);
            return result;
        }
        freeVal(value);
    }

    unpinPage(info->bm, pageHandle);
    free(pageHandle);
    return RC_OK;
}

// dealing with schemas
extern int getRecordSize (Schema *schema) {
    int size = 0;
    for (int i = 0; i < schema->numAttr; ++i) {
        DataType type = schema->dataTypes[i];
        switch (type) {
            case DT_INT:
                size += sizeof(int);
                break;
            case DT_BOOL:
                size += sizeof(bool);
                break;
            case DT_FLOAT:
                size += sizeof(float);
                break;
            case DT_STRING:
                size += schema->typeLength[i] + 1;
                break;
            default:
                break;
        }
    }
    return size;
}

extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    Schema *schema = calloc(1, sizeof(Schema));
    schema->keySize = keySize;
    schema->numAttr = numAttr;
    schema->typeLength = typeLength;
    schema->dataTypes = dataTypes;
    schema->keyAttrs = keys;
    schema->attrNames = attrNames;

    return schema;
}

extern RC freeSchema (Schema *schema) {
    free(schema->typeLength);
    free(schema->dataTypes);
    free(schema->keyAttrs);
    for (int i = 0; i < schema->numAttr; ++i) {
        free(schema->attrNames[i]);
    }
    free(schema->attrNames);
    free(schema);
    return RC_OK;
}

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema) {
    Record *newRecord = calloc(1, sizeof(Record));
    newRecord->data = malloc(sizeof(char) * getRecordSize(schema));
    memset(newRecord->data, 0, sizeof(char) * getRecordSize(schema));
    newRecord->id.page = 0;
    newRecord->id.slot = 0;
    *record = newRecord;

    return RC_OK;
}

extern RC freeRecord (Record *record) {
    free(record->data);
    free(record);
    return RC_OK;
}

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {
    int offset = 0;
    getAttrOffset(schema, attrNum, &offset);
    char *dataPointer = record->data;
    dataPointer += offset;

    Value *newValue = malloc(sizeof(Value));
    memset(newValue, 0, sizeof(Value));
    switch (schema->dataTypes[attrNum]) {
        case DT_STRING:
            newValue->dt = DT_STRING;
            newValue->v.stringV = malloc(sizeof(char) * (schema->typeLength[attrNum] + 1));
            memset(newValue->v.stringV, 0, sizeof(char) * (schema->typeLength[attrNum] + 1));
            strncpy(newValue->v.stringV, dataPointer, schema->typeLength[attrNum]);
            break;
        case DT_BOOL:
            newValue->dt = DT_BOOL;
            newValue->v.boolV = *(bool *)dataPointer;
            break;
        case DT_FLOAT:
            newValue->dt = DT_FLOAT;
            newValue->v.floatV = *(float *)dataPointer;
            break;
        case DT_INT:
            newValue->dt = DT_INT;
            newValue->v.intV = *(int *)dataPointer;
            break;
        default:
            return RC_RM_UNKOWN_DATATYPE;
    }

    *value = newValue;
    return RC_OK;
}

extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {
    int offset = 0;
    getAttrOffset(schema, attrNum, &offset);
    char *dataPointer = record->data + offset;
    switch (value->dt) {
        case DT_STRING:
            strncpy(dataPointer, value->v.stringV, schema->typeLength[attrNum]);
            break;
        case DT_BOOL:
            *(bool *)dataPointer = value->v.boolV;
            break;
        case DT_FLOAT:
            *(float *)dataPointer = value->v.floatV;
            break;
        case DT_INT:
            *(int *)dataPointer = value->v.intV;
            break;
        default:
            return RC_RM_UNKOWN_DATATYPE;
    }

    return RC_OK;
}

void getAttrOffset(const Schema *schema, int attrNum, int *offset) {

    for (int i = 0; i < attrNum; ++i) {
        switch (schema->dataTypes[i]) {
            case DT_STRING:
                *offset += (schema->typeLength[i] + 1);
                break;
            case DT_BOOL:
                *offset += sizeof(bool);
                break;
            case DT_FLOAT:
                *offset += sizeof(float);
                break;
            case DT_INT:
                *offset += sizeof(int);
                break;
            default:
                break;
        }
    }
}

SM_PageHandle schemaToPageHandle(Schema *pSchema) {
    char *data = calloc(PAGE_SIZE, sizeof(char));
    char *pageHandle = data;

    *(int *)pageHandle = pSchema->keySize;
    pageHandle += sizeof(int);

    *(int *)pageHandle = pSchema->numAttr;
    pageHandle += sizeof(int);

    for (int i = 0; i < pSchema->numAttr; ++i) {
        char *attrName = pSchema->attrNames[i];
        strcpy(pageHandle, attrName);
        pageHandle += ATTR_NAME_SIZE;

        *(DataType *)pageHandle = (DataType)pSchema->dataTypes[i];
        pageHandle += sizeof(DataType);

        *(int *)pageHandle = pSchema->typeLength[i];
        pageHandle += sizeof(int);
    }

    for (int i = 0; i < pSchema->keySize; ++i) {
        *(int *)pageHandle = pSchema->keyAttrs[i];
        pageHandle += sizeof(int);
    }

    return data;
}

Schema *pageHandleToSchema(BM_PageHandle *pHandle) {
    char *data = pHandle->data;
    int keySize = *(int *)data;
    data += sizeof(int);

    int numAttr = *(int *)data;
    data += sizeof(int);

    char **attrNames = malloc(sizeof(char *) * numAttr);
    memset(attrNames, 0, sizeof(char *) * numAttr);
    for(int i = 0; i < numAttr; i++) {
        attrNames[i]= (char*) malloc(ATTR_NAME_SIZE);
        memset(attrNames[i], 0, ATTR_NAME_SIZE);
    }


    DataType *dataTypes = calloc( numAttr, sizeof(DataType));

    int *keyAttrs = calloc( numAttr, sizeof(int));

    int *typeLength = calloc( numAttr, sizeof(int));

    for (int i = 0; i < numAttr; ++i) {
        strncpy(attrNames[i], data, ATTR_NAME_SIZE);
        data += ATTR_NAME_SIZE;

        dataTypes[i] = *(DataType *)data;
        data += sizeof(DataType);

        typeLength[i] = *(int *)data;
        data += sizeof(int);
    }

    for (int i = 0; i < keySize; ++i) {
        keyAttrs[i] = *(int *)data;
        data += sizeof(int);
    }

    return createSchema(numAttr, attrNames, dataTypes, typeLength, keySize, keyAttrs);
}

SM_PageHandle tableInfoToPageHandle(TableInfo *pInfo) {
    char *data = malloc(sizeof(char) * PAGE_SIZE);
    memset(data, 0, sizeof(char) * PAGE_SIZE);
    char *pageHandle = data;

    *(int *)pageHandle = pInfo->LastPageNum;
    pageHandle += sizeof(int);

    *(int *)pageHandle = pInfo->lastSlot;
    pageHandle += sizeof(int);

    *(int *)pageHandle = pInfo->recordSize;
    pageHandle += sizeof(int);

    *(int *)pageHandle = pInfo->numTuples;
    pageHandle += sizeof(int);

    *(int *)pageHandle = pInfo->maxSlot;

    return data;
}

void pageHandleToTableInfo(BM_PageHandle *pHandle, TableInfo *pInfo) {
    char *data = pHandle->data;

    pInfo->LastPageNum = *(int *)data;
    data += sizeof(int);

    pInfo->lastSlot = *(int *)data;
    data += sizeof(int);

    pInfo->recordSize = *(int *)data;
    data += sizeof(int);

    pInfo->numTuples = *(int *)data;
    data += sizeof(int);

    pInfo->maxSlot = *(int *)data;
}

RC writeRecord(Schema *schema, Record *record, BM_PageHandle *pageHandle) {
    RC result;
    Value *value;
    int offset = record->id.slot * getRecordSize(schema);
    char *data = pageHandle->data + HEADER_SIZE + offset;
    for (int i = 0; i < schema->numAttr; ++i) {
        if ((result = getAttr(record, schema, i, &value)) != RC_OK) {
            return result;
        }

        switch (value->dt) {
            case DT_STRING:
                strncpy(data, value->v.stringV, schema->typeLength[i]);
                data += (schema->typeLength[i]+1);
                break;
            case DT_INT:
                *(int *)data = value->v.intV;
                data += sizeof(int);
                break;
            case DT_FLOAT:
                *(float *)data = value->v.floatV;
                data += sizeof(float);
                break;
            case DT_BOOL:
                *(bool *)data = value->v.boolV;
                data += sizeof(bool);
                break;
            default:
                return RC_RM_UNKOWN_DATATYPE;
        }

        freeVal(value);
    }

    return RC_OK;
}

RC updateInfo(TableInfo *info, BM_PageHandle *pageHandle) {
    RC result;
    if ((result = pinPage(info->bm, pageHandle, 1)) != RC_OK){
        return result;
    }

    SM_PageHandle newInfo = tableInfoToPageHandle(info);
    memcpy(pageHandle->data, newInfo, PAGE_SIZE);
    markDirty(info->bm, pageHandle);
    unpinPage(info->bm, pageHandle);
    free(newInfo);
    return RC_OK;
}

// ----------------------- Scan functions ----------------------- //

extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    // Check if cond is null
    if (cond == NULL)
        return RC_RM_CONDITION_NOT_FOUND;

    // Initialize ScanManager
    ScanManager *scanManager = (ScanManager*) malloc(sizeof(ScanManager));

    // Initialize scan
    scan->rel = rel;
    scan->mgmtData = scanManager;

    // Set up metadata
    scanManager->rid.page = RECODER_START_PAGE;
    scanManager->rid.slot = 0;
    scanManager->cond = cond;
    scanManager->scannedTuples = 0;

    return RC_OK;
}

extern RC next(RM_ScanHandle *scan, Record *record)
{
    // Initialize
    ScanManager *scanManager = scan->mgmtData;
    Schema *schema = scan->rel->schema;
    TableInfo *tableInfo = scan->rel->mgmtData;

    // Check if cond is null
    if (scanManager->cond == NULL) {
        return RC_RM_CONDITION_NOT_FOUND;
    }

    if (tableInfo->numTuples < 1) {
        return RC_RM_NO_MORE_TUPLES;
    }

    Value *value;
    while(scanManager->scannedTuples <= tableInfo->numTuples){

        if (scanManager->scannedTuples <= 0)
        {
            scanManager->rid.page = RECODER_START_PAGE;
            scanManager->rid.slot = 0;
        }
        else
        {
            scanManager->rid.slot++;
            if(scanManager->rid.slot > tableInfo->maxSlot)
            {
                scanManager->rid.slot = 0;
                scanManager->rid.page++;
            }
        }

        record->id = scanManager->rid;
        RC result;
        if ((result = getRecord(scan->rel, record->id, record)) != RC_OK) {
            return result;
        }

        scanManager->scannedTuples++;
        evalExpr(record, schema, scanManager->cond, &value);
        if (value->v.boolV == TRUE)
        {
            freeVal(value);
            return RC_OK;
        }
        freeVal(value);
    }

    scanManager->rid.page = RECODER_START_PAGE;
    scanManager->rid.slot = 0;
    scanManager->scannedTuples = 0;
    return RC_RM_NO_MORE_TUPLES;

}

extern RC closeScan(RM_ScanHandle *scan)
{
    free(scan->mgmtData);
    return RC_OK;
}