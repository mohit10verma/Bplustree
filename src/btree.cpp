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
#include "exceptions/file_exists_exception.h"
#include "exceptions/page_not_pinned_exception.h"


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
        try {
            this->file = new BlobFile(outIndexName, true);

            Page* headerPage, *rootPage;

            this->bufMgr->allocPage(this->file, this->headerPageNum, headerPage);

            AllocatePageAndSetDefaultValues(this->rootPageNum, rootPage, false);


            //populate metadata of index header page
            IndexMetaInfo metainfo;
            strcpy(metainfo.relationName, relationName.c_str());
            metainfo.attrByteOffset = attrByteOffset;
            metainfo.attrType = attrType;
            metainfo.rootPageNo = this->rootPageNum;

            //Write metadata of index header page
            this->writeMetaInfoToPage(&metainfo, this->headerPageNum, headerPage);
            this->bufMgr->readPage(this->file, this->headerPageNum, headerPage);

            this->attributeType = attrType;
            this->attrByteOffset = attrByteOffset;
            this->leafOccupancy = 0;
            this->nodeOccupancy = 0;
            //TODO: initialize members specific to scanning
            //Construct Btree for this relation
            constructBtree(relationName);

        } catch (FileExistsException e)
        {
            this->file = new BlobFile(outIndexName, false);
            this->headerPageNum = 1; //TODO:assumed header page id id 1
            Page* headerPage;
            this->bufMgr->readPage(this->file, 1, headerPage);
            IndexMetaInfo* metaInfo = (IndexMetaInfo*) (&headerPage);

            this->rootPageNum = metaInfo->rootPageNo;
            this->attributeType = metaInfo->attrType;
            this->attrByteOffset = metaInfo->attrByteOffset;
            Page* rootPage;
            this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
        }
    }




// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
    void BTreeIndex::AllocatePageAndSetDefaultValues(PageId& pageNo, Page *&currPage, bool isLeaf){
        this->bufMgr->allocPage(this->file, pageNo, currPage);

        if(isLeaf){
            LeafNodeInt* leafNode = (LeafNodeInt*)currPage;
            for (int i = 0; i<INTARRAYLEAFSIZE;i++) {
                leafNode->keyArray[i] = INT32_MAX;
                leafNode->ridArray[i].page_number = UINT32_MAX;
                leafNode->ridArray[i].slot_number = UINT16_MAX;
            }
            leafNode->rightSibPageNo = -1;
        }
        else {
            NonLeafNodeInt* nonLeafNode = (NonLeafNodeInt*)currPage;
            for (int i = 0; i<INTARRAYNONLEAFSIZE;i++) {
                nonLeafNode->keyArray[i] = INT32_MAX;
                nonLeafNode->pageNoArray[i] = UINT32_MAX;
            }
            nonLeafNode->pageNoArray[INTARRAYNONLEAFSIZE] = UINT32_MAX;
            nonLeafNode->level = 1;
        }
    }

    BTreeIndex::~BTreeIndex() {
        this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
        this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
        this->bufMgr->flushFile(this->file);

        delete this->file;
    }

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
    const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
        //If root gets splitup, create new root, and update metapage.
        pair<int, PageId> p = this->findPageAndInsert(this->rootPageNum, key, rid, 0);
        if (p.first == -1 && p.second == UINT32_MAX) {
            //do nothing
        }
        else {
            this->bufMgr->unPinPage(this->file, this->rootPageNum, true);

            PageId newPageId;
            Page* newRootPage;
//            this->allocatePageAndUpdateMap(newPageId, 1);
            AllocatePageAndSetDefaultValues(newPageId, newRootPage, false);

            NonLeafNodeInt* newRootNode = (NonLeafNodeInt*) newRootPage;
            newRootNode->keyArray[0] = p.first;
            newRootNode->pageNoArray[0] = this->rootPageNum;
            newRootNode->pageNoArray[1] = p.second;
            newRootNode->level = 0;
            this->rootPageNum = newPageId;
            this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
            this->bufMgr->readPage(this->file, this->rootPageNum, newRootPage);

            Page *headerPage;
            this->bufMgr->readPage(this->file, this->headerPageNum, headerPage);
            IndexMetaInfo *metaInfo = (IndexMetaInfo*) (headerPage);
            metaInfo->rootPageNo = this->rootPageNum;
            this->writeMetaInfoToPage(metaInfo, this->headerPageNum, headerPage);
        }
        return;
    }

    PageId BTreeIndex::searchBtree(PageId pageNo, bool isLeafNode) {

        if (!isLeafNode) {

            Page *nonLeafPage;
            this->bufMgr->readPage(this->file, pageNo, nonLeafPage);
            NonLeafNodeInt* nonLeafNodeData = (NonLeafNodeInt*) nonLeafPage;
            int i=0;
            //TODO: Binary Search
            switch (this->lowOp) {
                case GT:
                    while ((this->lowValInt +1) >= nonLeafNodeData->keyArray[i] && i<INTARRAYNONLEAFSIZE) {
                        i++;
                    }
                    break;
                case GTE:
                    while (this->lowValInt >= nonLeafNodeData->keyArray[i] && i<INTARRAYNONLEAFSIZE) {
                        i++;
                    }
                    break;
                default:
                    assert(0);
            }

            PageId childPageId;
            int childLevel = nonLeafNodeData->level;
            if(i==0 && nonLeafNodeData->pageNoArray[i] == UINT32_MAX){
                childPageId = nonLeafNodeData->pageNoArray[i + 1];

            }
             else if (i!=0 &&nonLeafNodeData->pageNoArray[i] == UINT32_MAX ){
                childPageId = nonLeafNodeData->pageNoArray[i - 1];

            }  else {
                childPageId = nonLeafNodeData->pageNoArray[i ];

            }
            this->bufMgr->unPinPage(this->file, pageNo, false);
            return this->searchBtree(childPageId, childLevel);
            //Recursive search on the child Node

        }
        else {
            //reached the Leaf Node return the pageId
            return pageNo;
        }
    }
// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

    const void BTreeIndex::startScan(const void *lowValParm,
                                     const Operator lowOpParm,
                                     const void *highValParm,
                                     const Operator highOpParm) {

        this->lowValInt = *(int*)(lowValParm);
        this->highValInt = *(int*)(highValParm);

        if (this->lowValInt > this->highValInt) {
            throw BadScanrangeException();
        }

        if (highOpParm == GT || highOpParm == GTE || lowOpParm == LT || lowOpParm == LTE) {
            throw BadOpcodesException();
        }

        this->scanExecuting = true;
        this->lowOp = lowOpParm;
        this->highOp = highOpParm;
        //Start search on rootPage

        PageId foundLeafPage = searchBtree(this->rootPageNum, 0);

        this->currentPageNum = foundLeafPage;
        this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);

        LeafNodeInt* currentLeaf = (LeafNodeInt*) (this->currentPageData);
        int i = 0;
        switch (lowOpParm) {
            case GT:
                while (this->lowValInt+1 > currentLeaf->keyArray[i] && i < INTARRAYLEAFSIZE) {
                    i++;
                }
                break;
            case GTE:
                while (this->lowValInt > currentLeaf->keyArray[i] && i < INTARRAYLEAFSIZE) {
                    i++;
                }
            break;
            default:
                assert(0);
        }
        this->nextEntry = i;
        if (i == INTARRAYLEAFSIZE) {
            assert(0); //ASSERT here, not possible, must have the key,rid pair here
        }
        return;
    }

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

    const void BTreeIndex::scanNext(RecordId &outRid) {
        //Already know the leafPageId and data
        if (!this->scanExecuting) {
            throw ScanNotInitializedException();
        }
        LeafNodeInt* currentLeaf;
        currentLeaf = (LeafNodeInt*) (this->currentPageData);

        if (this->nextEntry == INTARRAYLEAFSIZE || currentLeaf->keyArray[this->nextEntry] == INT32_MAX ) {
            if (currentLeaf->rightSibPageNo != -1) {
                PageId  oldLeafNodeId = currentLeaf->rightSibPageNo;
                this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
                this->currentPageNum = oldLeafNodeId;

                this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
                currentLeaf = (LeafNodeInt*) (this->currentPageData);
                this->nextEntry = 0;
            }
            else {
                throw IndexScanCompletedException();
            }
        }

        //this->nextEntry must be valid here
        switch (this->highOp) {
            case LTE:
                if (currentLeaf->keyArray[this->nextEntry] <= this->highValInt) {
                    outRid = currentLeaf->ridArray[this->nextEntry];
                    this->nextEntry++;
                }
                else {
                    throw IndexScanCompletedException();
                }
            break;
            case LT:
                if (currentLeaf->keyArray[this->nextEntry] < this->highValInt) {
                    outRid = currentLeaf->ridArray[this->nextEntry];
                    this->nextEntry++;
                }
                else {
                    throw IndexScanCompletedException();
                }
            break;
            default:
                assert(0);
        }
    }

    typedef struct tuple {
        int i;
        double d;
        char s[64];
    } RECORD;

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
    const void BTreeIndex::endScan() {
        if (!this->scanExecuting) {
            throw ScanNotInitializedException();
        }
        this->scanExecuting = false;
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
    }

    //Assume this is type of structure of record

    int BTreeIndex::getKeyValue(FileIterator &file_it, PageIterator &page_it) {
        RecordId currRecordId = page_it.getCurrentRecord();
        string currRecord = (*file_it).getRecord(currRecordId);
        tuple *fetchedRecord;// = new tuple();
        fetchedRecord = (tuple*) (currRecord.c_str());
        //int keyValue = stoi(keyString);

        //std::cout<<keyString.c_str();
        int keyValue = fetchedRecord->i;//keyString[0];
        std::cout<<keyValue<<"\n";
        return keyValue;
    }

    void BTreeIndex::constructBtree(const std::string &relationName) {

        PageFile relation = PageFile::open(relationName);//open relation page file
        FileIterator file_it = relation.begin();//file begin, points to 1st page

        while (file_it != relation.end()) {
            PageIterator page_it = (*file_it).begin(); //page begin, points to 1st record
            while (page_it != (*file_it).end()) {
                RecordId currRecordId = page_it.getCurrentRecord();
                int keyValue = this->getKeyValue(file_it, page_it);
                this->insertEntry((void *) &keyValue, currRecordId);
                page_it++;
            }
            file_it++;
        }
        //Destructor of Pagefile closes the file. No explicit close
    }

    template <typename T>
    void shiftAndInsert(int *keyArray, T *TArray, int &currKey, const T &Tvalue, int keyArray_size, int array_size2) {
        //TODO: Binary search
        //Called only if node has space

        int i = 0;
        //TODO: can i exceed array size: check and update
        while (keyArray[i] < currKey) {
            i++;
        }
        //empty slot, no shifting required, insert and return
        if (keyArray[i] == INT32_MAX) {
            keyArray[i] = currKey;
            if (array_size2==keyArray_size) {//For leaf node
                TArray[i] = Tvalue;
            }
            else{//For nonleaf node
                TArray[i+1] = Tvalue;
            }
            return;
        }

        for (int k = keyArray_size - 1; k > i; k--) {
            keyArray[k] = keyArray[k - 1];
        }

        if (keyArray_size == array_size2) {//For leaf node
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

    void BTreeIndex::insertFirstEntryInRoot(NonLeafNodeInt *rootNode, int currKey, const RecordId rid) {
        //Alocate child node and populate default contents
        rootNode->keyArray[0] = currKey;
        Page* firstLeafPage;
        AllocatePageAndSetDefaultValues(rootNode->pageNoArray[1], firstLeafPage, true);


        LeafNodeInt* newLeafNode = (LeafNodeInt*) firstLeafPage;
        newLeafNode->keyArray[0] = currKey;
        newLeafNode->ridArray[0] = rid;
        this->bufMgr->unPinPage(this->file, rootNode->pageNoArray[1], true);
        return;
    }

    void BTreeIndex::writeMetaInfoToPage(IndexMetaInfo *metaNode, PageId metaLeafPageID, Page* headerPage) {
        memcpy((void*) headerPage, (void *) metaNode, sizeof(IndexMetaInfo));
        this->bufMgr->unPinPage(this->file, metaLeafPageID, true);
    }

//Very good job
    template<typename T>
    void BTreeIndex::writeNodeToPage(T *newNode, PageId pageId, Page* page) {
        //memcpy((void*) page, (void *) newNode, sizeof(T));
        try {
            this->bufMgr->unPinPage(this->file, pageId, true);
        }
        catch (PageNotPinnedException) {
            std::cout << "Trying to unpin a page with pin Cnt = 0" << std::endl;
            assert(0);
        }
    }

    template<typename T>
    bool BTreeIndex::isNodeFull(T *node, int size) {
        if (node->keyArray[size-1] != INT32_MAX) {
            return true;
        }
        else {
            return false;
        }
    }

    //for leaf node
    void BTreeIndex::copyAndSet(LeafNodeInt* newLeafNode, LeafNodeInt* currentNode, int start, int size)
    {
        int copyKeySize = size* sizeof(int);
        int copyRidSize = size* sizeof(RecordId);
        memcpy((void *) &newLeafNode->keyArray[0], (void *) &currentNode->keyArray[start],
               copyKeySize);
        memcpy((void *) &newLeafNode->ridArray[0], (void *) &currentNode->ridArray[start],
               copyRidSize);
        for (int i = 0; i< size; i++) {
            currentNode->keyArray[i+start] = INT32_MAX;
            currentNode->ridArray[i+start].page_number = UINT32_MAX;
            currentNode->ridArray[i+start].slot_number = UINT16_MAX;
        }
    }

    //For non-leaf node
    void BTreeIndex::copyAndSet(NonLeafNodeInt* newNonLeafNode, NonLeafNodeInt* currentNode, int start, int size)
    {
        int copyKeySize = size* sizeof(int);
        int copyPageIdSize = size* sizeof(PageId);
        memcpy((void *) &newNonLeafNode->keyArray[0], (void *) &currentNode->keyArray[start],
               copyKeySize);
        memcpy((void *) &newNonLeafNode->pageNoArray[0], (void *) &currentNode->pageNoArray[start],
               copyPageIdSize + sizeof(PageId));
        for (int i = 0; i< size; i++) {
            currentNode->keyArray[i + start] = INT32_MAX;
            currentNode->pageNoArray[i + start + 1] = UINT32_MAX;
        }
        //currentNode->pageNoArray[INTARRAYNONLEAFSIZE] = UINT32_MAX;
    }

    int BTreeIndex::splitLeafNodeInTwo(LeafNodeInt* newLeafNode, LeafNodeInt* currentNode, RecordId r, int k)
    {
        int i = 0;
        while (currentNode->keyArray[i] < k && i < INTARRAYLEAFSIZE) {
            i++;
        }
        //i = new position
        if (i<INTARRAYLEAFSIZE/2) {
            int start = floor(INTARRAYLEAFSIZE/2.0);
            this->copyAndSet(newLeafNode,currentNode, start, INTARRAYLEAFSIZE - start );
            shiftAndInsert(currentNode->keyArray, currentNode->ridArray, k, r, INTARRAYLEAFSIZE, INTARRAYLEAFSIZE);

        }
        else{
            int start = ceil(INTARRAYLEAFSIZE/2.0);
            this->copyAndSet(newLeafNode,currentNode, start , INTARRAYLEAFSIZE - start);
            shiftAndInsert(newLeafNode->keyArray, newLeafNode->ridArray, k, r, INTARRAYLEAFSIZE, INTARRAYLEAFSIZE);
        }
        return newLeafNode->keyArray[0];
    }



    int BTreeIndex::splitNonLeafNode(NonLeafNodeInt* newNonLeafNode, NonLeafNodeInt* currentNode, int key, PageId pageId){
        int i = 0;
        while (currentNode->keyArray[i] < key && i< INTARRAYNONLEAFSIZE) {
            i++;
        }
        //Copy the level
        newNonLeafNode->level = currentNode->level;
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
            //newnode has the pointer set as pageNoArray was split into two
            return newkey;
        }
        else{
            this->copyAndSet(newNonLeafNode, currentNode, INTARRAYNONLEAFSIZE/2 , INTARRAYNONLEAFSIZE - INTARRAYNONLEAFSIZE/2);
            shiftAndInsert(newNonLeafNode->keyArray, newNonLeafNode->pageNoArray,
                           key, pageId, INTARRAYNONLEAFSIZE, INTARRAYNONLEAFSIZE+1);
            int newkey = newNonLeafNode->keyArray[0];
            int counter;
            for(counter=0; counter<INTARRAYNONLEAFSIZE-1; counter++){
                newNonLeafNode->keyArray[counter] = newNonLeafNode->keyArray[counter+1];
                newNonLeafNode->pageNoArray[counter] = newNonLeafNode->pageNoArray[counter+1];
                //Check everywhere but not here for segFault
            }

            newNonLeafNode->pageNoArray[counter] = UINT32_MAX;
            return newkey;

        }
    }

    pair<int, PageId> BTreeIndex::findPageAndInsert(PageId currPageId, const void *key, const RecordId rid, bool isLeafNode) {
        int currentKey = *(int *) key;

        Page *currPage;
        this->bufMgr->readPage(this->file, currPageId, currPage);

        if (!isLeafNode) {
            NonLeafNodeInt *currentNode = (NonLeafNodeInt *) currPage;
            //Nonleaf node---------------------------------------------------------------
            //Check if root page and empty , insert in root nad leaf
            if (currPageId == this->rootPageNum) {
                int isRootEmpty = this->isRootPageEmpty(currentNode);

                if (isRootEmpty) {
                    this->insertFirstEntryInRoot(currentNode, currentKey, rid);
                    this->bufMgr->unPinPage(this->file, currPageId, true);
                    return pair<int, PageId>(-1, UINT32_MAX);
                }
            }
            // Find the child node index and navigate to the child through recursion
            int i = 0;
            while (currentNode->keyArray[i] < currentKey && i<INTARRAYNONLEAFSIZE) {
                i++;
            }
            // i is the pagenumber where it should go!
            // Note: i = INTARRAYNONLEAFSIZE in worst case, and it is okay to access pageNoArray[INTARRAYNONLEAFSIZE]
            if (currentNode->pageNoArray[i] == UINT32_MAX) {
                //Child page doesn't exist, create it
                assert(currentNode->level == 1);//Just below current Node are the leaf Nodes
                int counter = i;
                Page* newLeafPage;
                AllocatePageAndSetDefaultValues(currentNode->pageNoArray[i], newLeafPage, true);

                int rightpageID = currentNode->pageNoArray[i];
                if( counter != 0) {
                    counter--;//finding left sibling
                    if(currentNode->pageNoArray[counter] ==UINT32_MAX){
                        assert(0);
                    }
                    else{
                        //left sibling present, yay!!!
                        Page* leftChildPage;
                        this->bufMgr->readPage(this->file, currentNode->pageNoArray[counter], leftChildPage);
                        LeafNodeInt *leftchild = (LeafNodeInt *) leftChildPage;
                        //Swapping siblingPageIDs
                        PageId currentRightSibling = leftchild->rightSibPageNo;
                        leftchild->rightSibPageNo = rightpageID;

                        LeafNodeInt* justCreatedLeaf = (LeafNodeInt*) newLeafPage;
                        justCreatedLeaf->rightSibPageNo = currentRightSibling;
                        this->bufMgr->unPinPage(this->file, currentNode->pageNoArray[i], true);
                        this->bufMgr->unPinPage(this->file, currentNode->pageNoArray[counter], true);
                    }
                }
                else if (counter == 0) {
                    //Left leaf child allocated for the 1st time
                    assert(currentNode->pageNoArray[1] != UINT32_MAX );
                    LeafNodeInt justCreatedLeaf;
                    justCreatedLeaf.rightSibPageNo = currentNode->pageNoArray[1];
                    this->bufMgr->unPinPage(this->file, currentNode->pageNoArray[0], true);
                }

            }
            pair<int, PageId> childReturn = this->findPageAndInsert(currentNode->pageNoArray[i], key, rid, currentNode->level);

            //Check return type , if -1,UINT32_MAX then return
            if (childReturn.first == -1 && childReturn.second == UINT32_MAX) {
                //Unpin before returning
                //Possiblity of pinCount Mismatch
                this->bufMgr->unPinPage(this->file, currPageId, true);
                return pair<int, PageId>(-1, UINT32_MAX);
            }
                //Else Child node was split, copy the key to current node
            else {
                int isFull = this->isNodeFull(currentNode, INTARRAYNONLEAFSIZE);

                if(isFull){
                    //If no space in current node split the current node and push up
                    PageId newPageId;
                    Page* newPage;
                    //this->allocatePageAndUpdateMap(newPageId, 1);

                    AllocatePageAndSetDefaultValues(newPageId, newPage, false);

                    NonLeafNodeInt* newNonLeafNode = (NonLeafNodeInt*) newPage;
                    int newKey = this->splitNonLeafNode(newNonLeafNode, currentNode, childReturn.first, childReturn.second);
                    this->bufMgr->unPinPage(this->file, newPageId, true);
                    this->bufMgr->unPinPage(this->file, currPageId, true);
                    return pair<int, PageId > (newKey, newPageId);
                }
                else{
                    //Else insert
                    shiftAndInsert(currentNode->keyArray, currentNode->pageNoArray, childReturn.first, childReturn.second, INTARRAYNONLEAFSIZE, INTARRAYNONLEAFSIZE+1);

                    this->bufMgr->unPinPage(this->file, currPageId, true);
                    return pair<int, PageId>(-1, UINT32_MAX);
                }
            }
        }
        else {
            //Leaf node
            LeafNodeInt *currentNode = (LeafNodeInt *) currPage;

            if (currentNode->keyArray[INTARRAYLEAFSIZE - 1] != INT32_MAX) {
                //Split and copy up, no space in leaf
                PageId newLeafPageID;
                Page* newLeafPage;
                AllocatePageAndSetDefaultValues(newLeafPageID, newLeafPage, true);

                LeafNodeInt* newLeafNode = (LeafNodeInt*) newLeafPage;

                //Split the node contents to a new page
                int newKey = this->splitLeafNodeInTwo(newLeafNode, currentNode, rid, currentKey);
                PageId currentSiblingPageId = currentNode->rightSibPageNo;

                //Swapping siblingPageIDs

                currentNode->rightSibPageNo = newLeafPageID;
                newLeafNode->rightSibPageNo = currentSiblingPageId;
                //Typecast to string and write the new page
                this->bufMgr->unPinPage(this->file, newLeafPageID, true);
                this->bufMgr->unPinPage(this->file, currPageId, true);
                return pair<int, PageId>(newKey, newLeafPageID);
            }
            else {
                //Find where to insert, and shift
                shiftAndInsert(&currentNode->keyArray[0], &currentNode->ridArray[0], currentKey, rid,
                               INTARRAYLEAFSIZE, INTARRAYLEAFSIZE);
                this->bufMgr->unPinPage(this->file, currPageId, true);
                return pair<int, PageId>(-1, UINT32_MAX);
            }
        }
    }
}
