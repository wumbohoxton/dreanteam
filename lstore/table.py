from lstore.index import Index
from time import time
from lstore.page import Page, PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

MAX_BASE_PAGES = 16
METADATA_COLUMNS = 4

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

        # pass
        # initalizing table
        self.page_ranges.append(PageRange(self.total_columns)) # create initial page range
        # self.page_ranges[0]

        
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
            if not page_range.base_pages[page_range.basePageToWrite][i].has_capacity(len(values[i])):
                page_range.basePageToWrite += 1
                break
        # if the page range is full, then allocate a new page range
        if page_range.basePageToWrite >= MAX_BASE_PAGES:
            self.page_ranges.append(PageRange(self.total_columns))
            page_range = self.page_ranges[len(self.page_ranges)-1]

        # can safely write the entire base record into the base page of the selected page range
        page_offsets = [None] * self.total_columns # save the page offsets for each column for later
        for i in range(self.total_columns):
            page_offsets[i] = page_range.base_pages[page_range.basePageToWrite][i].page_size
            page_range.base_pages[page_range.basePageToWrite][i].write(values[i])

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
 
