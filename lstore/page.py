CAPACITY = 4096
MANDATORY_COLUMNS = 4
MAX_BASE_PAGES = 16

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray()
        self.page_number = 0
        self.page_size = 0

    def has_capacity(self, size):
        return self.page_size + size < CAPACITY

    def write(self, value: bytes):
        if self.has_capacity(len(value)):
            value.to_bytes()
            self.data[self.page_size:self.page_size + len(value)] = value
            self.page_size += len(value)
            return True
        return False
    
    # todo create a read function, needs to be able to account for different data types fml
        
class PageRange:

    def __init__(self, num_columns): # initialize 16 base pages indexed at 0
        self.base_pages = []
        for base_page in range(0, MAX_BASE_PAGES - 1): # make 16 base pages
            for page in range(0, (num_columns + MANDATORY_COLUMNS - 1)): 
                self.base_pages[base_page][page] = Page(page) # create a page for each column
            
        self.tail_pages = []
        for page in range(0, num_columns + MANDATORY_COLUMNS - 1): #create one tail page
            self.tail_pages[0][page] = Page(page)


