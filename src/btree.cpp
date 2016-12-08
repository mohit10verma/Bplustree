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

            Page headerPage;
            //Page*& page
//            this->bufMgr->allocPage(this->file, this->headerPageNum, headerPage);
            headerPage = this->file->allocatePage(this->headerPageNum);
            this->allocatePageAndUpdateMap(this->rootPageNum, 1);
            //populate metadata of index header page
            IndexMetaInfo metainfo;
            strcpy(metainfo.relationName, relationName.c_str());
            metainfo.attrByteOffset = attrByteOffset;
            metainfo.attrType = attrType;
            metainfo.rootPageNo = this->rootPageNum;

            //Write metadata of index header page
//            this->bufMgr->unPinPage(this->file,this->headerPageNum, true);
            this->writeMetaInfoToPage(&metainfo, this->headerPageNum);


            NonLeafNodeInt rootNode;
            this->writeNodeToPage(&rootNode, this->rootPageNum);

            this->attributeType = attrType;
            this->attrByteOffset = attrByteOffset;
            this->leafOccupancy = 0;
            this->nodeOccupancy = 0;
            //TODO: initialize members specific to scanning
            //Construct Btree for this relation
            constructBtree(relationName);

            Page currPage = this->file->readPage(this->headerPageNum);
            //Page *currPage;
//            this->bufMgr->readPage(this->file,this->headerPageNum, currPage);
            IndexMetaInfo *metaInfo = (IndexMetaInfo*) (&currPage);
            metaInfo->rootPageNo = this->rootPageNum;
//            this->bufMgr->unPinPage(this->file,this->headerPageNum, true);
            this->writeMetaInfoToPage(metaInfo, this->headerPageNum);

            /*TODO: remove this*/
            Page firstPage = this->file->readPage(this->rootPageNum);
            NonLeafNodeInt *firstNode = (NonLeafNodeInt*) &firstPage;
            Page firstChild = this->file->readPage(firstNode->pageNoArray[1]);
            LeafNodeInt *firstChildNode = (LeafNodeInt*) &firstChild;
            this->file->writePage(this->rootPageNum,firstPage);
            this->file->writePage(firstNode->pageNoArray[1],firstChild);

            cout<<"here"<<endl;

        } catch (FileExistsException e)
        {
            this->file = new BlobFile(outIndexName, false);
            this->headerPageNum = 1; //TODO:assumed header page id id 1
            Page headerPage = this->file->readPage(1);//TODO: assumed header page id is 1
            IndexMetaInfo* metaInfo = (IndexMetaInfo*) (&headerPage);

            this->rootPageNum = metaInfo->rootPageNo;
            this->attributeType = metaInfo->attrType;
            this->attrByteOffset = metaInfo->attrByteOffset;
            Page firstPage = this->file->readPage(this->rootPageNum);
            NonLeafNodeInt *firstNode = (NonLeafNodeInt*) &firstPage;
            Page firstChild = this->file->readPage(firstNode->pageNoArray[1]);
            LeafNodeInt *firstChildNode = (LeafNodeInt*) &firstChild;
            cout<<"here"<<endl;
        }
    }




// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

    BTreeIndex::~BTreeIndex() {
        //this->bufMgr->flushFile(this->file);
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
            PageId newPageId;
            this->allocatePageAndUpdateMap(newPageId, 1);
            NonLeafNodeInt newRootNode;
            newRootNode.keyArray[0] = p.first;
            newRootNode.pageNoArray[0] = this->rootPageNum;
            newRootNode.pageNoArray[1] = p.second;
            newRootNode.level = 0;
            this->rootPageNum = newPageId;
            this->writeNodeToPage(&newRootNode, this->rootPageNum);
            Page currPage = this->file->readPage(this->headerPageNum);
            IndexMetaInfo *metaInfo = (IndexMetaInfo*) (&currPage);
            metaInfo->rootPageNo = this->rootPageNum;
            this->writeMetaInfoToPage(metaInfo, this->headerPageNum);
        }
        return;
    }

    PageId BTreeIndex::searchBtree(PageId pageNo, bool isLeafNode) {

        if (!isLeafNode) {
            //TODO: NonLeaf Node search
            Page nonLeafPage = this->file->readPage(pageNo);
            NonLeafNodeInt* nonLeafNodeData = (NonLeafNodeInt*) &nonLeafPage;
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


            if(i==0 && nonLeafNodeData->pageNoArray[i] == UINT32_MAX){
                return this->searchBtree(nonLeafNodeData->pageNoArray[i + 1], nonLeafNodeData->level);
            }
             else if (i!=0 &&nonLeafNodeData->pageNoArray[i] == UINT32_MAX ){
                return this->searchBtree(nonLeafNodeData->pageNoArray[i-1], nonLeafNodeData->level);
            }  else {
                return this->searchBtree(nonLeafNodeData->pageNoArray[i], nonLeafNodeData->level);
            }
            //Recursive search on the child Node

        }
        else {
            //TODO: reached the Leaf Node return the pageId
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
        this->currentPageData = new Page();
        *this->currentPageData = this->file->readPage(this->currentPageNum);
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
                this->currentPageNum = currentLeaf->rightSibPageNo;
                *this->currentPageData = this->file->readPage(this->currentPageNum);
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
        delete this->currentPageData;
    }

    void BTreeIndex::allocatePageAndUpdateMap(PageId& page_number, int isNonLeaf) {
        this->file->allocatePage(page_number);

        //this->pageTypeMap.insert(pair<PageId, int>(page_number, isNonLeaf));
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

    void BTreeIndex::insertEntryInRoot(NonLeafNodeInt *rootNode, int currKey, const RecordId rid) {
        //Alocate child node and populate default contents
        rootNode->keyArray[0] = currKey;
        this->file->allocatePage(rootNode->pageNoArray[1]);

        LeafNodeInt newLeafNode;
        newLeafNode.keyArray[0] = currKey;
        newLeafNode.ridArray[0] = rid;

        writeNodeToPage(&newLeafNode, rootNode->pageNoArray[1]);
        //this->bufMgr->

        writeNodeToPage(rootNode, this->rootPageNum);
        return;
    }

    void BTreeIndex::writeMetaInfoToPage(IndexMetaInfo *metaNode, PageId metaLeafPageID) {
        Page localPage;
        memcpy((void*) &localPage, (void *) metaNode, sizeof(IndexMetaInfo));
        this->file->writePage(metaLeafPageID, localPage);
    }

//Very good job
    template<typename T>
    void BTreeIndex::writeNodeToPage(T *newNode, PageId newLeafPageID) {
        //char infoArray[sizeof(T)];
        Page localPage;
        memcpy((void*) &localPage, (void *) newNode, sizeof(T));
        //localPage.insertRecord(string(infoArray));
        this->file->writePage(newLeafPageID, localPage);
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
            //TODO:Confirm newnode has the pointer set as pageNoArray was split into two
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
        Page currPage = this->file->readPage(currPageId);

        if (!isLeafNode) {
            NonLeafNodeInt *currentNode = (NonLeafNodeInt *) &currPage;
            //Nonleaf node---------------------------------------------------------------
            //Check if root page and empty , insert in root nad leaf
            if (currPageId == this->rootPageNum) {
                int isRootEmpty = this->isRootPageEmpty(currentNode);

                if (isRootEmpty) {
                    this->insertEntryInRoot(currentNode, currentKey, rid);
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
                this->allocatePageAndUpdateMap(currentNode->pageNoArray[i], 0);//Create leaf page
                int counter = i;
                int rightpageID = currentNode->pageNoArray[i];
                if( counter != 0) {
                    counter--;//finding left sibling
                    if(currentNode->pageNoArray[counter] ==UINT32_MAX){
                        assert(0);
                    }
                    else{
                        //left sibling present, yay!!!
                        Page leftchildPage = this->file->readPage(currentNode->pageNoArray[counter]);
                        LeafNodeInt *leftchild = (LeafNodeInt *) &leftchildPage;
                        //Swapping siblingPageIDs
                        PageId currentRightSibling = leftchild->rightSibPageNo;
                        leftchild->rightSibPageNo = rightpageID;
                        writeNodeToPage(leftchild, currentNode->pageNoArray[counter]);
                        LeafNodeInt justCreatedLeaf;
                        justCreatedLeaf.rightSibPageNo = currentRightSibling;
                        writeNodeToPage(&justCreatedLeaf,currentNode->pageNoArray[i]);
                    }
                }
                else if (counter == 0) {
                    //Left leaf child allocated for the 1st time
                    assert(currentNode->pageNoArray[1] != UINT32_MAX );
                    LeafNodeInt justCreatedLeaf;
                    justCreatedLeaf.rightSibPageNo = currentNode->pageNoArray[1];
                    writeNodeToPage(&justCreatedLeaf,currentNode->pageNoArray[0]);
                }
                writeNodeToPage(currentNode,currPageId);
            }
            pair<int, PageId> childReturn = this->findPageAndInsert(currentNode->pageNoArray[i], key, rid, currentNode->level);

            //Check return type , if -1,UINT32_MAX then return
            if (childReturn.first == -1 && childReturn.second == UINT32_MAX) {
                return pair<int, PageId>(-1, UINT32_MAX);
            }
                //Else Child node was split, copy the key to current node
            else {
                int isFull = this->isNodeFull(currentNode, INTARRAYNONLEAFSIZE);

                if(isFull){
                    //If no space in current node split the current node and push up
                    PageId newPageId;
                    this->allocatePageAndUpdateMap(newPageId, 1);
                    NonLeafNodeInt newNonLeafNode;
                    int newKey = this->splitNonLeafNode(&newNonLeafNode, currentNode, childReturn.first, childReturn.second);
                    writeNodeToPage(&newNonLeafNode, newPageId);
                    writeNodeToPage(currentNode,currPageId);
                    return pair<int, PageId > (newKey, newPageId);
                }
                else{
                    //Else insert
                    shiftAndInsert(currentNode->keyArray, currentNode->pageNoArray, childReturn.first, childReturn.second, INTARRAYNONLEAFSIZE, INTARRAYNONLEAFSIZE+1);
                    writeNodeToPage(currentNode, currPageId);
                    return pair<int, PageId>(-1, UINT32_MAX);
                }
            }
        }
        else {
            //Leaf node
            LeafNodeInt *currentNode = (LeafNodeInt *) &currPage;

            if (currentNode->keyArray[INTARRAYLEAFSIZE - 1] != INT32_MAX) {
                //Split and copy up, no space in leaf
                PageId newLeafPageID;
                this->allocatePageAndUpdateMap(newLeafPageID, 0);
                LeafNodeInt newLeafNode;

                //Split the node contents to a new page
                int newKey = this->splitLeafNodeInTwo(&newLeafNode, currentNode, rid, currentKey);
                PageId currentSiblingPageId = currentNode->rightSibPageNo;

                //Swapping siblingPageIDs

                currentNode->rightSibPageNo = newLeafPageID;
                newLeafNode.rightSibPageNo = currentSiblingPageId;
                //Typecast to string and write the new page
                writeNodeToPage(&newLeafNode, newLeafPageID);
                writeNodeToPage(currentNode, currPageId);
                return pair<int, PageId>(newKey, newLeafPageID);
            }
            else {
                //Find where to insert, and shift
                shiftAndInsert(&currentNode->keyArray[0], &currentNode->ridArray[0], currentKey, rid,
                               INTARRAYLEAFSIZE, INTARRAYLEAFSIZE);
                writeNodeToPage(currentNode, currPageId);
                return pair<int, PageId>(-1, UINT32_MAX);
            }
        }
    }
}
