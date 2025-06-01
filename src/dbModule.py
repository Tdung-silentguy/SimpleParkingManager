import pandas as pd
import duckdb as dk
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
# UTILITIES
def addMonth(arr):
    '''
    this method will add 1 month to splited datetime\n
    Params:
        3-element arr include:
        arr[0] is day
        arr[1] is month
        arr[2] is year
    Returns:
        list 
    '''
    num = int(arr[1])
    num += 1
    if num > 12:
        arr[1] = '01'
        arr[2] += 1
    arr[1] = f"{num:02d}"
    return arr

class DBObject:
    def __init__(self, path_):
        self.path = path_
        self.data = pd.read_csv(self.path)
        self.con = dk.connect()
        self.con.register("my_table", self.data)
        
    def reload_data(self):
        self.data = pd.read_csv(self.path)
        
    def print_table(self):
        print(self.data.to_string())
        
    def add_entry(self, so_the, bien_so, mssv, bat_dau, ket_thuc):
        newrow_df = pd.DataFrame({
            'so_the': [so_the],
            'bien_so': [bien_so],
            'mssv': [mssv],
            'bat_dau': [bat_dau],
            'ket_thuc': [ket_thuc]
        })
        newrow_df.to_csv(self.path, mode='a', index=False, header=False)
        self.reload_data()
        
    def lookup_entry(self, key, mode):
        # mode=0 -> search for bien_so; mode=1 -> search for mssv
        if mode == 0:
            return self.con.execute("SELECT * FROM my_table WHERE bien_so=?", [key]).fetchdf()
        elif mode == 1:
            return self.con.execute("SELECT * FROM my_table WHERE mssv=?", [key]).fetchdf()
        
    def contains(self, key):
        df = self.con.execute("SELECT * FROM my_table WHERE so_the=?", [key]).fetchdf()
        return not(df.empty)
    
    def extend(self, key):
        # key is ensured to be in the table

        '''df = self.con.execute("SELECT * FROM my_table").fetchdf()
        idxList = df.index[df['so_the'] == key].tolist()
        row = idxList[0]'''
        df = self.con.execute("SELECT * FROM my_table WHERE so_the=?", [key]).fetchdf() # this will select the vehicle
        row = df.index[0]

        endDate = df.at[0, "ket_thuc"] # ket_thuc is 4th column
        arr = str(endDate).split('/') # the ket_thuc format is dd//mm/yyyy with( arr[0] is day, arr[1] is month, arr[2] is year)
        
        # for profit's sake, we only add 1 to the month, not 30 days
        '''if arr[0] <= 28:
            arr = addMonth(arr)
        elif arr[1]=='''
        arr = addMonth(arr)
        newEndDate = '/'.join(arr)
        self.data.loc[row, "ket_thuc"] = newEndDate
        self.data.to_csv(self.path, index=False)
        self.reload_data()
    
    def remove_vehicle(self, key, mode = 0):
        '''
        this function will untracking the selected vehicle in the parking
        Params:
            key: the selected key to deleted
            mode:
                if mode = 0 it will select by the 'bien_so' \n
                if mode = 1 it will select by mssv
        Returns:
            None
        '''
        if(mode == 0):      
            # check existance
            df = self.con.execute("SELECT * FROM my_table WHERE bien_so=?", [key]).fetchdf()
            if df.empty:
                raise ValueError('key does not exist ')
            self.data = self.data[self.data['bien_so'] != key]
        elif (mode == 1):
            df = self.con.execute("SELECT * FROM my_table WHERE mssv=?", [key]).fetchdf()
            if df.empty:
                raise ValueError('key does not exist ')
            self.data = self.data[self.data['mssv'] != key]
        else:
            raise ValueError('invalid mode')
        self.data.to_csv(self.path, index=False)
        self.reload_data()
        
'''# testSE
DATA_PATH = 'assets/xe.csv'
dbo = DBObject(DATA_PATH)
result = dbo.lookup_entry('4', 0)'''