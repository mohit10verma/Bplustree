/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

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
namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
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
		this->file->allocatePage(this->rootPageNum);

		//populate metadata of index header page
		IndexMetaInfo metainfo;
		strcpy(metainfo.relationName , relationName.c_str());
		metainfo.attrByteOffset = attrByteOffset;
		metainfo.attrType = attrType;
		metainfo.rootPageNo = this->rootPageNum;

		//Write metadata of index header page
		char metainfoarray[sizeof(metainfo)];
		memcpy(metainfoarray, (char *) &metainfo, sizeof(metainfo));
		Page localmetapage;
		localmetapage.insertRecord(string(metainfoarray));
		this->file->writePage(this->headerPageNum,localmetapage);

		//populate rootpage
		this->populateNewLeafPage(this->rootPageNum);

		this->attributeType = attrType;
		this->attrByteOffset = attrByteOffset;
		this->leafOccupancy = 0;
		this->nodeOccupancy = 0;
		//TODO: initialize members specific to scanning
		//Construct Btree for this relation
		constructBtree(relationName);
	}
}

void BTreeIndex::populateNewNonLeafPage(PageId page_number) {
	NonLeafNodeInt node;
	node.level = 1;//TODO: check this
	memset((void*)node.keyArray, INT32_MAX, INTARRAYNONLEAFSIZE);
	memset((void*)node.pageNoArray, INT32_MAX, INTARRAYNONLEAFSIZE+1);
	//write node data to page
	char infoarray[sizeof(NonLeafNodeInt)];
	memcpy(infoarray, (char*) &node, sizeof(node));
	Page localPage;
	localPage.insertRecord(string(infoarray));
	this->file->writePage(page_number,localPage);
}

void BTreeIndex::populateNewLeafPage(PageId page_number) {
	LeafNodeInt node;

	memset((void*)node.keyArray, INT32_MAX, INTARRAYLEAFSIZE);
	memset((void*)node.ridArray, INT32_MAX, INTARRAYLEAFSIZE+1);
	node.rightSibPageNo = INT32_MAX;
	//write node data to page
	char infoarray[sizeof(LeafNodeInt)];
	memcpy(infoarray, (char*) &node, sizeof(node));
	Page localPage;
	localPage.insertRecord(string(infoarray));
	this->file->writePage(page_number,localPage);
}

int BTreeIndex::getKeyValue(FileIterator& file_it, PageIterator& page_it) {
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
			this->insertEntry((void*) &keyValue, currRecordId);
			page_it++;
		}
		file_it++;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	Page currPage = this->file->readPage(this->rootPageNum);
	int CurrentKey = *(int*)key;
	NonLeafNodeInt* currentNode =(NonLeafNodeInt*) &currPage;
	for(int i = 0; i< INTARRAYNONLEAFSIZE ; i++) {
		if(CurrentKey  < currentNode->keyArray[i]  ){
			continue;
		}
		else{//Found position
			//TODO: shift data & insert key, will return left child index
			int child_index;
			if(currentNode->pageNoArray[child_index] != INT32_MAX ){

				//Insert rid pair
			}
			else{
				//Create a leaf node and insert the rid
				this->file->allocatePage(currentNode->pageNoArray[child_index]);
				//initialize leaf page
				this->populateNewLeafPage(currentNode->pageNoArray[child_index]);

			}

		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
