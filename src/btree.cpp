/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <bits/algorithmfwd.h>
#include <math.h>
#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG
using namespace std;
namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

    BTreeIndex::BTreeIndex(const std::string &relationName,
                           std::string &outIndexName,
                           BufMgr *bufMgrIn,
                           const int attrByteOffset,
                           const Datatype attrType) {
        this->bufMgr = bufMgrIn;
        outIndexName = relationName + "." + std::to_string(attrByteOffset);
        if (File::exists(outIndexName)) {
            static BlobFile indexfile = BlobFile::open(outIndexName);
            this->file = &indexfile;//TODO: remove this
        }
        else {
            static BlobFile indexfile = BlobFile::create(outIndexName);
            this->file = &indexfile;//TODO: remove this

            this->file->allocatePage(this->headerPageNum);
            this->allocatePageAndUpdateMap(this->rootPageNum, 1);

            //populate metadata of index header page
            IndexMetaInfo metainfo;
            strcpy(metainfo.relationName, relationName.c_str());
            metainfo.attrByteOffset = attrByteOffset;
            metainfo.attrType = attrType;
            metainfo.rootPageNo = this->rootPageNum;

            //Write metadata of index header page
            this->writeNodetoPage(&metainfo, this->headerPageNum);




            this->attributeType = attrType;
            this->attrByteOffset = attrByteOffset;
            this->leafOccupancy = 0;
            this->nodeOccupancy = 0;
            //TODO: initialize members specific to scanning
            //Construct Btree for this relation
            constructBtree(relationName);
        }
    }


    void BTreeIndex::allocatePageAndUpdateMap(PageId& page_number, int isNonLeaf) {
        this->file->allocatePage(page_number);

        this->pagetypemap.insert(pair<int, int>(page_number, isNonLeaf));
    }

    int BTreeIndex::getKeyValue(FileIterator &file_it, PageIterator &page_it) {
        RecordId currRecordId = page_it.getCurrentRecord();
        string currRecord = (*file_it).getRecord(currRecordId);
        int keyValue = stoi(currRecord.substr(this->attrByteOffset, sizeof(int)));
        return keyValue;
    }

    void BTreeIndex::constructBtree(const std::string &relationName) {
        FileIterator file_it = PageFile::open(relationName).begin();//file begin, points to 1st page

        while (file_it != PageFile::open(relationName).end()) {
            PageIterator page_it = (*file_it).begin(); //page begin, points to 1st record
            while (page_it != (*file_it).end()) {
                RecordId currRecordId = page_it.getCurrentRecord();
                int keyValue = this->getKeyValue(file_it, page_it);
                this->insertEntry((void *) &keyValue, currRecordId);
                page_it++;
            }
            file_it++;
        }
    }

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

    BTreeIndex::~BTreeIndex() {
    }

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

    template <typename T>
    void shiftAndInsert(int *keyArray, T *TArray, int &currKey, const T &Tvalue, int array_size1, int array_size2) {
        //TODO: Binary search
        //Called only if node has space

        int i = 0;
        while (keyArray[i] < currKey) {
            i++;
        }
        //empty slot, no shifting required
        if (keyArray[i] == INT32_MAX) {
            keyArray[i] = currKey;
            if (array_size2==array_size1) {//For leaf node
                TArray[i] = Tvalue;
            }
            else{//For nonleaf node
                TArray[i+1] = Tvalue;
            }
        }

        for (int k = array_size1 - 1; k > i; k--) {
            keyArray[k] = keyArray[k - 1];
        }

        if (array_size1 == array_size2) {//For leaf node
            for (int j = array_size2 - 1; j > i; j--) {
                TArray[j] = TArray[j - 1];
            }
            keyArray[i] = currKey;
            TArray[i] = Tvalue;
        }
        else {// for NonLeafNode
            for (int j = array_size2 - 1; j > i+1; j--) {
                TArray[j] = TArray[j - 1];
            }
            keyArray[i] = currKey;
            TArray[i+1] = Tvalue;
        }

        return;
    }

    bool BTreeIndex::isRootPageEmpty(NonLeafNodeInt *rootNode) {

        if (rootNode->keyArray[0] == INT32_MAX) {
            return true;
        }
        else {
            return false;
        }
    }

    void BTreeIndex::insertEntryInRoot(NonLeafNodeInt *rootNode, int currKey, const RecordId rid) {
        //Alocate child node and populate default contents
        this->file->allocatePage(rootNode->pageNoArray[1]);

        LeafNodeInt newLeafNode;
        newLeafNode.keyArray[0] = currKey;
        newLeafNode.ridArray[0] = rid;

        writeNodetoPage(&newLeafNode, rootNode->pageNoArray[1]);

        rootNode->keyArray[0] = currKey;
        writeNodetoPage(rootNode, this->rootPageNum);
        return;
    }

//Very good jjob
    template<typename T>
    void BTreeIndex::writeNodetoPage(T *newNode, PageId newLeafPageID) {
        char infoArray[sizeof(T)];
        memcpy(infoArray, (char *) &newNode, sizeof(newNode));
        Page localPage;
        localPage.insertRecord(string(infoArray));
        this->file->writePage(newLeafPageID, localPage);
    }

    template<typename T>
    bool BTreeIndex::isNodeFull(T *node, int size) {
        if (node->keyArray[size] != INT32_MAX) {
            return false;
        }
        else {
            return true;
        }
    }

    //for leaf node
    void BTreeIndex::copyAndSet(LeafNodeInt* newLeafNode, LeafNodeInt* currentNode, int start, int size)
    {
        size = size* sizeof(int);
        memcpy((void *) &newLeafNode->keyArray[0], (void *) &currentNode->keyArray[start],
               size);
        memcpy((void *) &newLeafNode->ridArray[0], (void *) &currentNode->ridArray[start],
               size);
        memset((void *) &currentNode->keyArray[start], INT32_MAX, size);
        memset((void *) &currentNode->ridArray[start], INT32_MAX, size);
    }

    //For non-leaf node
    void BTreeIndex::copyAndSet(NonLeafNodeInt* newLeafNode, NonLeafNodeInt* currentNode, int start, int size)
    {
        size = size* sizeof(int);
        memcpy((void *) &newLeafNode->keyArray[0], (void *) &currentNode->keyArray[start],
               size);
        memcpy((void *) &newLeafNode->pageNoArray[0], (void *) &currentNode->pageNoArray[start],
               size+ sizeof(int));
        memset((void *) &currentNode->keyArray[start], INT32_MAX, size);
        memset((void *) &currentNode->pageNoArray[start+1], INT32_MAX, size );
    }

    int BTreeIndex::splitLeafNodeInTwo(LeafNodeInt* newLeafNode, LeafNodeInt* currentNode, RecordId r, int k)
    {
        int i = 0;
        while (currentNode->keyArray[i] < k) {
            i++;
        }
        //i = new position
        if (i<INTARRAYLEAFSIZE/2) {
            int start = floor(INTARRAYLEAFSIZE/2);
            this->copyAndSet(newLeafNode,currentNode, start, INTARRAYLEAFSIZE - start );
            shiftAndInsert(currentNode->keyArray, currentNode->ridArray, k, r, INTARRAYLEAFSIZE, INTARRAYLEAFSIZE);

        }
        else{
            int start = ceil(INTARRAYLEAFSIZE/2);
            this->copyAndSet(newLeafNode,currentNode, start , INTARRAYLEAFSIZE - start);
            shiftAndInsert(newLeafNode->keyArray, newLeafNode->ridArray, k, r, INTARRAYLEAFSIZE, INTARRAYLEAFSIZE);
        }
        return newLeafNode->keyArray[0];
    }



    int BTreeIndex::splitNonLeafNode(NonLeafNodeInt* newNonLeafNode, NonLeafNodeInt* currentNode, int key, PageId pageId){
        int i = 0;
        while (currentNode->keyArray[i] < key) {
            i++;
        }
        //i = Position to insert
        if (i == INTARRAYNONLEAFSIZE/2) {
            //Do not insert, same key should be returned back
            this->copyAndSet(newNonLeafNode, currentNode, INTARRAYNONLEAFSIZE/2, INTARRAYNONLEAFSIZE - INTARRAYNONLEAFSIZE/2);
            newNonLeafNode->pageNoArray[0] = pageId;
            return key;
        }
        if (i < INTARRAYNONLEAFSIZE/2) {

            this->copyAndSet(newNonLeafNode, currentNode, INTARRAYNONLEAFSIZE/2, INTARRAYNONLEAFSIZE - INTARRAYNONLEAFSIZE/2);

            shiftAndInsert(currentNode->keyArray, currentNode->pageNoArray,
                           key, pageId, INTARRAYNONLEAFSIZE, INTARRAYNONLEAFSIZE+1);
            int newkey = currentNode->keyArray[INTARRAYNONLEAFSIZE/2 ];
            currentNode->keyArray[INTARRAYNONLEAFSIZE/2 ] = INT32_MAX;

            currentNode->pageNoArray[INTARRAYNONLEAFSIZE/2 ] = pageId;
            //TODO:Confirm newnode has the pointer set as pageNoArray was split into two
            return newkey;

        }
        else{
            this->copyAndSet(newNonLeafNode, currentNode, INTARRAYNONLEAFSIZE/2, INTARRAYNONLEAFSIZE - INTARRAYNONLEAFSIZE/2);
            shiftAndInsert(newNonLeafNode->keyArray, newNonLeafNode->pageNoArray,
                           key, pageId, INTARRAYNONLEAFSIZE, INTARRAYNONLEAFSIZE+1);
            int newkey = currentNode->keyArray[0];
            for(int counter=0; counter<INTARRAYNONLEAFSIZE-2; counter++){
                newNonLeafNode->keyArray[i] = newNonLeafNode->keyArray[i+1];
                //Check everywhere but not here for segFault
            }
            newNonLeafNode->pageNoArray[0] = pageId;
            return newkey;

        }
    }

    pair<int, PageId> BTreeIndex::findpageandinsert(PageId currPageId, const void *key, const RecordId rid) {
        int currentKey = *(int *) key;
        Page currPage = this->file->readPage(currPageId);
        if (pagetypemap[currPageId] == 1) {
            NonLeafNodeInt *currentNode = (NonLeafNodeInt *) &currPage;
            //Nonleaf node---------------------------------------------------------------
            //Check if root page and empty , insert in root nad leaf
            if (currPageId == this->rootPageNum) {
                int isRootEmpty = this->isRootPageEmpty(currentNode);

                if (isRootEmpty) {
                    this->insertEntryInRoot(currentNode, currentKey, rid);
                    return pair<int, PageId>(-1, -1);
                }
            }
            // Find the child node index and navigate to the child through recurssion
            int i = 0;
            while (currentNode->keyArray[i] < currentKey) {
                i++;
            }
            // i is the pagenumber where it should go!

            pair<int, PageId> childReturn = findpageandinsert(currentNode->pageNoArray[i], key, rid);

            //Check return type , if -1,-1 then return
            if (childReturn.first == -1 && childReturn.second == -1) {
                return pair<int, PageId>(-1, -1);
            }
                //TODO:Else Child node was split, copy the key to current node
            else {
                int isfull = this->isNodeFull(currentNode, INTARRAYNONLEAFSIZE);


                if(isfull){

                    //TODO: IF no space in current node split the current node and push up
                    PageId newPageId;
                    this->allocatePageAndUpdateMap(newPageId, 1);
                    NonLeafNodeInt newNonLeafNode;
                    int newKey = this->splitNonLeafNode(&newNonLeafNode, currentNode, childReturn.first, childReturn.second);
                    return pair<int, PageId > (newKey, newPageId);
                }
                else{
                    //Else insert
                    shiftAndInsert(currentNode->keyArray, currentNode->pageNoArray, currentKey, childReturn.second, INTARRAYNONLEAFSIZE, INTARRAYNONLEAFSIZE+1);
                }
            }

        }
        else {
            //Leaf node
            LeafNodeInt *currentNode = (LeafNodeInt *) &currPage;

            if (currentNode->keyArray[INTARRAYLEAFSIZE - 1] != INT32_MAX) {
                //Split and copy up, no space in leaf
                PageId newLeafPageID;
                this->allocatePageAndUpdateMap(newLeafPageID, 1);
                LeafNodeInt newLeafNode;

                //Split the node contents to a new page
                int newKey = this->splitLeafNodeInTwo(&newLeafNode, currentNode, rid, currentKey);

                //Typecast to string and write the new page
                writeNodetoPage(&newLeafNode, newLeafPageID);
                writeNodetoPage(currentNode, currPageId);
                return pair<int, PageId>(newKey, newLeafPageID);
            }
            else {
                //Find where to insert, and shift
                shiftAndInsert(&currentNode->keyArray[0], &currentNode->ridArray[0], currentKey, rid,
                               INTARRAYLEAFSIZE, INTARRAYLEAFSIZE);
                writeNodetoPage(currentNode, currPageId);
                return pair<int, PageId>(-1, -1);
            }
        }
    }

    const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
        //TODO: If root gets splitup, create new root, and update metapage.
        pair<int, PageId> p = this->findpageandinsert(this->rootPageNum, key, rid);
        
    }

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

    const void BTreeIndex::startScan(const void *lowValParm,
                                     const Operator lowOpParm,
                                     const void *highValParm,
                                     const Operator highOpParm) {

    }

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

    const void BTreeIndex::scanNext(RecordId &outRid) {

    }

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
    const void BTreeIndex::endScan() {

    }

}
