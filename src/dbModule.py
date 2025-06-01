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
    # regenerate date time from arr
    date = '/'.join(arr)
    date = datetime.strptime(date, "%d/%m/%Y")
    new_date = date + relativedelta(months=1)
    new_date = new_date.strftime("%d/%m/%Y")
    arr = str(new_date).split('/')
    return arr

class DBObject:
    def __init__(self, path_):
        self.path = path_
        self.data = pd.read_csv(self.path)
        self.con = dk.connect()
        self.con.register("my_table", self.data)
        
    def reload_data(self):
        '''
        reload file and connection to duckdb
        '''
        self.con.unregister("my_table")
        self.data = pd.read_csv(self.path)
        self.con.register("my_table", self.data)
        
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
    
    def extend(self, card_id: str):
        '''
        this method extend expired day of selected vehicle via card id
        '''
        
        # check valid argument
        df = self.con.execute("SELECT * FROM my_table WHERE so_the=?", [card_id]).fetchdf() # this will select the vehicle
        if df.empty:
            raise ValueError('this card number doesn\'t exist')
        
        expire_date = df['ket_thuc'][0] #get the first row because card_id is unique
        expire_date = datetime.strptime(expire_date, '%d/%m/%Y')
        
        expire_date = expire_date + relativedelta(months=1)
        expire_date = datetime.strftime(expire_date, '%d/%m/%Y')
        self.data.loc[self.data['so_the'] ==  int(card_id), 'ket_thuc'] = expire_date # due to type of card_id is str it need to 
        # convert to int before compare with value in data['so_the']
    
        self.data.to_csv(self.path, index= False)
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