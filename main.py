''' 
        IMPLEMENTATION DATE : 25/06/2019

        DEVELOPER NAME : M.BALAVIGNESH

        SCOPE OF IMPLEMENTATION : To Backup the POSTGRESQL Database from needed host in the form of sql file
                                  Then Restore in the local System.
'''

from postgresinspector import postgresterminator as psql 
from filehelper import filehandlers as fileh
from config.config import ConfigBase as config


class Main:
    psql_obj = ''
    psql_con = ''
    fileh_obj = ''

    #Constrcutor
    def __init__(self):
        db_name = str(input("Enter the DB Name: "))
        db_user = str(input("Enter the DB User: "))
        db_host = str(input("Enter the DB Host: ")
        db_pass = str(input("Enter the DB User Password: "))
        self.psql_obj = psql.PostgresManager(db_name, db_user, db_host, db_pass)
        self.fileh_obj = fileh.FileHandler()

    def GetDbConObject(self):
        res = self.psql_obj.DBConMaker(self.psql_obj.db_name,
                                 self.psql_obj.db_user,
                                 self.psql_obj.db_host,
                                 self.psql_obj.db_pass)
        if res[0]:
            self.psql_con = res[1]
            self.psql_obj.DBMessageCatcher("DB Connection Established Successfully..")

    def MakeDBackup(self):
        req_file_name = str(input("Enter the Backup DB Name: "))
        res = self.psql_obj.DB_Backuper(req_file_name,
                                  self.psql_obj.db_user, 
                                  self.psql_obj.db_host, 
                                  self.psql_obj.db_pass, 
                                  self.psql_obj.db_name)
        if res:
            self.psql_obj.DBMessageCatcher("DB Backup Successfully Created...")
            path=config.get("sub-dir-1")+config.get("root")+config.get("sql_backup")+config.get("root")
            dump_file = "{path}{db_name}_db.sql".format(path=path,db_name=req_file_name)
            self.psql_obj.DUMP_DB_Size(dump_file)
        else:
            self.psql_obj.DBMessageCatcher("DB Backup Failed...")

    def GetDBSize(self):
        tar_db = str(input("Enter the DB Name: "))
        res = self.psql_obj.DB_SizeInspector(self.psql_obj.db_user,
                                             self.psql_obj.db_host,
                                             self.psql_obj.db_pass,
                                             tar_db)
        if res[0]:
            self.psql_obj.DBMessageCatcher("DB: {x} Size is: {y}-{z}".format(x=tar_db, y=res[1],z=res[2]))
        else:
            self.psql_obj.DBMessageCatcher("Not Success")

    def GetAllBackupDB(self):
        files = self.fileh_obj.ListDirFiles(config.get("sub-dir-1")+config.get("root")+config.get("sql_backup")+config.get("root"))
        i = 1
        for f in files:
            print("{index}. {f}".format(index=i,f=f))
            i = i+1

    def RestoreDB(self):
        choice = int(input("Enter the Index of the Backup DB: "))
        newdb_name = str(input("Enter the New DatabaseName: "))
        back_up_name = self.fileh_obj.GetFileIndexName(config.get("sub-dir-1")+config.get("root")+config.get("sql_backup")+config.get("root"),choice)
        data_cre_res = self.psql_obj.CREATE_DB(self.psql_obj.db_user,
                                               self.psql_obj.db_host,
                                               self.psql_obj.db_pass,
                                                newdb_name)
        if data_cre_res:
            restore_status = self.psql_obj.RestoreSQL(self.psql_con.db_user,
                                                      self.psql_obj.db_host,
                                                      self.psql_obj.db_pass,
                                                      newdb_name,back_up_name)
            if restore_status:
                self.psql_obj.DBMessageCatcher("Required DB Restored Successfully...")
            else:
                self.psql_obj.DBMessageCatcher("Required DB Restore Failed...")

        else:
            self.psql_obj.DBMessageCatcher("Database Creation Failed...")

    def Shutdown(self):
        quit()


            
    
if __name__ == "__main__":
    main = Main()
    main.GetDbConObject()
    def TargetSyncronizer(choice):
        {
         1 : main.GetDBSize,
         2 : main.GetAllBackupDB,
         3 : main.MakeDBackup,
         4 : main.RestoreDB,
         5 : main.Shutdown
        }.get(choice)()

    print("Choice: 1 -Show DB Size")
    print("Choice: 2 -Show Backup DB")
    print("Chocie: 3 -Make Backup")
    print("Choice: 4- Restore The Backup")
    print("Choice: 5- Exit")
    print(" ")
    while True:
        choice = int(input("Enter Your Choice: "))
        TargetSyncronizer(choice)





    
