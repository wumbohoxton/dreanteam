from lstore.index import Index
from time import time
from lstore.page import Page, PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

MAX_BASE_PAGES = 16
METADATA_COLUMNS = 4
ENTRY_SIZE = 8 # 8 bytes

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key # the value of the primary key of the record
        self.columns = columns # columns: a list of the values corresponding to each column

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns 
        self.total_columns = num_columns + METADATA_COLUMNS # + 4 for rid, indirection, schema, timestamping. these 4 columns are internal to table
        self.page_directory = {} # key: column, RID --> value: page_range, page, page_offset
        self.page_ranges = []
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge

        self.RID_counter = 0 # counter for assigning RIDs
        self.page_ranges.append(PageRange(self.total_columns)) # create initial page range


        
    """
    # insert an entirely new record. this goes into a base page
    # columns: an array of the columns with values we want to insert. does not include the 4 metadata columns so we need to calculate those ourselves
    """
    def insert_new_record(self, columns):
        page_range = self.page_ranges[len(self.page_ranges)-1] # the page range we want to write to is the last one in the array
        # initialize an array with the complete list of data values to insert (metadata values + the record's values)
        values = [None] * METADATA_COLUMNS
        values[INDIRECTION_COLUMN] = None # not needed but included for clarity
        values[RID_COLUMN] = self.getNewRID()
        values[TIMESTAMP_COLUMN] = time.ctime(time.time()) 
        values[SCHEMA_ENCODING_COLUMN] = '0' * self.table.num_columns
        values += columns

        for i in range(self.total_columns):
            # check if the base page fully has room for each column. if any of them don't, we need to move on to the next base page
            if not page_range.base_pages[page_range.basePageToWrite][i].has_capacity(len(values[i])): # len(values[i]) is future proofing lol -DH
                page_range.basePageToWrite += 1
                break
        # if the page range is full, then allocate a new page range
        if page_range.basePageToWrite >= MAX_BASE_PAGES:
            self.page_ranges.append(PageRange(self.total_columns))
            page_range = self.page_ranges[len(self.page_ranges)-1]

        # ---- THIS NEEDS TO BE REVISITED as all milestones have all columns as 64 bit integers, no strings or anything
        # can safely write the entire base record into the base page of the selected page range
        page_offsets = [None] * self.total_columns # save the page offsets for each column for later
        for i in range(self.total_columns):
            page_offsets[i] = page_range.base_pages[page_range.basePageToWrite][i].page_size
            page_range.base_pages[page_range.basePageToWrite][i].write(values[i])
        # ----------------------------------------------------------------------


        # add the values to the index. for now just index the primary key
        self.index.insert_record(values[RID_COLUMN], values[self.key], self.key)

        # add the mapping to the page directory
        page_range_index = len(self.page_ranges)-1
        for i in range(self.total_columns):
            # the page range index is always the same.
            # the page is always the same since a record is aligned across the base page thus requiring them all to be written on the same page index in their columns
            # use the page offsets that were saved earlier
            self.page_directory[(i, values[RID_COLUMN])] = (page_range_index, page_range.basePageToWrite, page_offsets[i])
        return True
    
    """
    # update a record. append a tail page, update indirection columns as needed
    # columns: an array of the columns with values we want the record to be updated to. does not include the 4 metadata columns so we need to calculate those ourselves
    """
    def update_record(self, primary_key, columns):
        RIDs = self.index.locate(self.key, primary_key) 
        if len(RIDs) == 0: # record does not exist
            return False 
        # if the record exists there should only be one item in the list because primary keys are unique
        baseRID = RIDs[0]

        # read the indirection value's location in the base pages
        # we do this because we need to access the old version of the record and also link up the new tail page with the old tail page pointer
        indirection_pointer = self.read(INDIRECTION_COLUMN, baseRID)

        # initialize an array with the complete list of data values to insert (metadata values + the record's values)
        values = [None] * METADATA_COLUMNS
        values[INDIRECTION_COLUMN] = None # not needed but included for clarity
        values[RID_COLUMN] = self.getNewRID()
        values[TIMESTAMP_COLUMN] = time.ctime(time.time()) 

        # set the schema encoding bits
        schema_encoding = ""
        for i in range(len(columns)):
            # this column is being updated
            if (columns[i] != None):
                schema_encoding += '1'
            # column not being updated
            else:
                schema_encoding += '0'
        values[SCHEMA_ENCODING_COLUMN] = schema_encoding

        # update the base record's schema encoding bits
        base_schema = self.read(SCHEMA_ENCODING_COLUMN, baseRID)
        base_schema_encoding = ''

        for i in range(len(columns)):
            if columns[i] == '1' or base_schema == '1': # checks if new update updates x column or previous updates have previously done so 
                base_schema_encoding += '1'
            else:
                base_schema_encoding += '0'
                
        self.replace(baseRID, SCHEMA_ENCODING_COLUMN, base_schema_encoding)

        # ready the tail record
        values += columns

        # this would be the first update to the record, so we need to 
        # (1) change the base's indirection pointer from None to our RID 
        # (2) set the new tail page's indirection to the base page's RID
        if indirection_pointer == None:
            self.replace(baseRID, RID_COLUMN, values[RID_COLUMN])
            values[INDIRECTION_COLUMN] = baseRID # (2)
        # if this isn't the first update, then
        # (1) set the new tail page's indirection to what was previously contained in the base page
        # (2) update the base page's indirection to point to the new tail page's RID
        else:
            values[INDIRECTION_COLUMN] = indirection_pointer # (1)
            self.replace(baseRID, RID_COLUMN, values[RID_COLUMN]) # (2)

        # inserting a new tail record
        # check each column. if it is the first update of that column, we need to insert a tail record that copies the original data
        # no matter what, we always have to insert the actual new tail record
        
        # get the page range from the base page
        page_range = self.page_directory.get((RID_COLUMN, baseRID))[0] # uses base RID, RID_COLUMN as a random column to access the page range index of the base record
        last_tail_page = len(page_range.tail_page) - 1
        for i in range(self.total_columns):
            # check if the last tail page fully has room for each column. if any of them don't, we need to allocate a new tail page
            if not page_range.tail_pages[page_range.tail_page[last_tail_page]][i].has_capacity(len(values[i])): # len(values[i]) is future proofing  -DH
                page_range.allocate_new_tail_page() 
                last_tail_page = len(page_range.tail_page) - 1 # using new tail page
                break
        
        # can safely write the entire tail record into the tail page of the selected page range
        page_offsets = [None] * self.total_columns # save the page offsets for each column for later
        for i in range(self.total_columns):
            page_offsets[i] = page_range.tail_pages[last_tail_page][i].page_size # done so since page sizes might differ due to None values
            page_range.tail_pages[last_tail_page][i].write(values[i]) #write record to the last tail page       

        # add the values to the index. for now just index the primary key, no secondary keys right now
        self.index.insert_record(values[RID_COLUMN], values[self.key], self.key)

        # add the mapping to the page directory
        page_range_index = self.page_ranges.index(page_range) # find the index of the page range we've been looking at
        for i in range(self.total_columns):
            # the page range index is the one our base record being updated is in
            # the page is the first available tail page, so the last one 
            # use the page offsets that were saved earlier 
            self.page_directory[(i, values[RID_COLUMN])] = (page_range_index, last_tail_page, page_offsets[i])

        return True

    # replaces value in specified column and RID
    def replace(self, RID, column_for_replace, value): 
        location = self.page_directory.get((column_for_replace, RID))
        page_range_index = location[0]
        base_page_index = location[1]
        page_offset = location[2]
        self.page_ranges[page_range_index][base_page_index].replace(value, page_offset)

    # passes in column and RID desired, gets address of page range, base page, page offset, returns value 
    def read(self, column_to_read, RID):
        location = self.page_directory.get((column_to_read, RID))
        page_range_index = location[0]
        base_page_index = location[1]
        page_offset = location[2]
        read_value = self.page_ranges[page_range_index][base_page_index].read(page_offset)
        return read_value

    def getNewRID(self):
        RID = self.RID_counter
        self.RID_counter += 1
        return RID


    # def page_range_init(self):
    #     for column in 

    #     pass

    def __merge(self):
        print("merge is happening")
        pass
 
