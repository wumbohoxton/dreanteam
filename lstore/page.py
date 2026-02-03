from lstore.table import Record

CAPACITY = 4096
MANDATORY_COLUMNS = 4
MAX_BASE_PAGES = 16

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray()
        self.page_number = 0
        self.page_size = 0
        self.data_size = 0 # the size of the data type this page stores
        
    def has_capacity(self, size):
        return self.page_size + size < CAPACITY

    def write(self, value: bytes):
        if self.has_capacity(len(value)):
            value.to_bytes()
            self.data[self.page_size:self.page_size + len(value)] = value
            self.page_size += len(value)
            if self.data_size == 0:
                self.data_size = self.page_size # on the first write, the page size is the same as the data size
            return True
        return False
    
    def read(self, lower_index):
        upper_index = lower_index + self.data_size
        # this should always pass since we don't write unless we have the full capacity needed, but just in case
        if upper_index < CAPACITY:
            return(self.data[lower_index:upper_index])
        else:
            return None
        
class PageRange:

    def __init__(self, num_columns): # initialize 16 base pages indexed at 0
        self.total_columns = num_columns + MANDATORY_COLUMNS
        self.basePageToWrite = 0 # a variable that keeps count of the current base page we should write to

        self.base_pages = []
        for base_page in range(0, MAX_BASE_PAGES): # make 16 base pages
            for page in range(0, (self.total_columns)): 
                self.base_pages[base_page][page] = Page(page) # create a page for each column

        self.tail_pages = []
        self.allocate_new_tail_page()

    def allocate_new_tail_page(self):
        newTailPage = []
        for page in range(0, self.total_columns): # create one tail page
            newTailPage.append(Page(page))
        self.tail_pages.append(newTailPage)


