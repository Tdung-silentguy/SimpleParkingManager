import dbModule as dbm
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_PATH = BASE_DIR / 'assets' / 'xe.csv'
HELP_PATH = BASE_DIR / 'assets' / 'command_list.txt'

class UserInterface:
    def __init__(self):
        self.status = 0
        self.dbo = dbm.DBObject(DATA_PATH)
        
    def greet(self):
        print("SPM: SIMPLE PARKING MANAGER version 0.1 Alpha\nAuthor: tdung")

# COMMAND HANDLERS
    def help_handler(self):
        print("'help' is being developed")
        f = open(HELP_PATH, 'r')
        print(f.read())
        f.close()

    def exit_handler(self):
        self.status = 0
        print("SPM terminated. Thank you for using our service!")

    def invalid_handler(self):
        print("Invalid command! Enter 'help' for list of commands, 'exit' for exit")

    def list_handler(self):
        self.dbo.print_table()
    def clear_handler(self):
        os.system('cls' if os.name == 'nt' else 'clear')
    def add_handler(self):
        #l = len(args)
        
    #if (self.enoughArgs(l, 5)):
        args = ['so_the', 'bien_so', 'mssv', 'bat_dau', 'ket_thuc']
        for i in range(5):
            inp = str(input(">>>>>> Enter " + args[i] + ": "))
            while not(self.is_legit_input(i, inp)):
                print("Illegal input argument!")
                inp = str(input(">>>>>> " + args[i] + ": "))
            args[i] = inp
        self.dbo.add_entry(args[0], args[1], args[2], args[3], args[4])
        print("Vehicle added!")
    def delete_handler(self) -> None:
        '''
        this method handle the delete vehicle by mssv or the bien so from the user
        Returns:
            None
        '''
        user_input = input('Enter mssv or bien so: ')
        if(len(user_input) == 6):
            result = self.dbo.remove_vehicle(user_input, 1)
        else:
            result = self.dbo.remove_vehicle(user_input, 0)
        
        print('Delete successfully')

        
    def search_handler(self):
        arg = str(input(">>>>>> Enter bien_so or mssv: "))
        result = None
        if len(arg) == 6:
            result = self.dbo.lookup_entry(arg, 1)
        else:
            result = self.dbo.lookup_entry(arg, 0)
        print(result)
    def extend_handler(self):
        arg = str(input(">>>>>> Enter so_the: "))
        if self.dbo.contains(arg):
            self.dbo.extend(arg)
            print("Added 1 month for vehicle", arg)
        else:
            print("ERROR: Vehicle not found!")
        

        

# UTILITIES
    def enoughArgs(self, len, expected):
        if len == expected:
            return True
        if len < expected:
            print("Missing arguments! Enter 'help' for list of commands, 'exit' for exit")
        else:
            print("Redundant arguments! Enter 'help' for list of commands, 'exit' for exit")
        return False
    def raiseError(self, err):
        print("ERROR:", err)
# RUN INTERFACE

    def run_interface(self):
        self.status = 1
        self.clear_handler()
        self.greet()

        while (self.status):
            args = str(input(">>> Enter command: ")).strip().split()
            match args[0]:
                case 'help':
                    self.help_handler()
                case 'exit':
                    self.exit_handler()
                case 'list':
                    self.list_handler()
                case 'clear':
                    self.clear_handler()
                case 'add':
                    self.add_handler()
                case 'delete':
                    self.delete_handler()
                case 'search':
                    self.search_handler()
                case 'extend':
                    self.extend_handler()
                case _:
                    self.invalid_handler()

