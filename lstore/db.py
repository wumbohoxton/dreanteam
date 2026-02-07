from lstore import table
from lstore.table import Table

class Database():

    def __init__(self):
        #self.tables = []
        self.tables = {} # dictionary for faster lookup alternative?

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):

        # Prevent duplicate table names 
        if self.get_table(name) is not None:
            print(f"dupe table name: '{name}' already exists")
            return None

        table = Table(name, num_columns, key_index)
        #self.tables.append(table)

        self.tables[name] = table # dictionary alternative

        return table # assuming it wants the table returned instead of boolean

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        table = self.get_table(name)
        if table is None: # prevent ValueError in case of non-existant table
            print(f"table '{name}' DNE")
            return False
        #self.tables.remove(table)

        del self.tables[name]  # dictionary alternative

        return True

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        #for table in self.tables:
        #    if table.name == name:
        #        return table
            
        return self.tables.get(name, None) # dictionary alternative

        #return None
