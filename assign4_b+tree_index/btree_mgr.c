//
// Created by gvt on 2022/12/8.
//
#include "btree_mgr.h"

// init and shutdown index manager
extern RC initIndexManager (void *mgmtData){
    return RC_OK;
}
extern RC shutdownIndexManager () {
    return RC_OK;
}

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n){
    return RC_OK;
}

extern RC openBtree (BTreeHandle **tree, char *idxId){
    return RC_OK;
}

extern RC closeBtree (BTreeHandle *tree) {
    return RC_OK;
}

extern RC deleteBtree (char *idxId){
    return RC_OK;
}

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result){
    return RC_OK;
}

extern RC getNumEntries (BTreeHandle *tree, int *result) {
    return RC_OK;
}

extern RC getKeyType (BTreeHandle *tree, DataType *result) {
    return RC_OK;
}

// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result) {
    return RC_OK;
}

extern RC insertKey (BTreeHandle *tree, Value *key, RID rid) {
    return RC_OK;
}

extern RC deleteKey (BTreeHandle *tree, Value *key) {
    return RC_OK;
}

extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle) {
    return RC_OK;
}

extern RC nextEntry (BT_ScanHandle *handle, RID *result) {
    return RC_OK;
}

extern RC closeTreeScan (BT_ScanHandle *handle) {
    return RC_OK;
}

// debug and test functions
extern char *printTree (BTreeHandle *tree) {
    return NULL;
}