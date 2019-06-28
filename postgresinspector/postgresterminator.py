import psycopg2 as psql
import datetime
from subprocess import Popen, run, PIPE
from config.config import ErrorFlags, ExceptionStr
from filehelper import getfilesize as files


class PostgresManager:

    #Constructor
    def __init__(self, db_name=None, db_user=None, db_host=None, db_pass=None):
        self.db_name = db_name
        self.db_user = db_user
        self.db_host = db_host
        self.db_pass = db_pass
        self.fileinfo = files.FileSizeManager()
    
    #General Message printing Method
    def DBMessageCatcher(self, mess):
        print(" ")
        print(mess)


    def AskDBName(self):
        db_name = str(input("Enter the Database Name: "))
        self.DBConMaker(db_name, self.db_user, self.db_host, self.db_pass)

    def AskHostName(self):
        db_host = str(input("Enter the Database Host: "))
        self.DBConMaker(self.db_name, self.db_user, db_host, self.db_pass)


    def AskUserCredit(self):
        db_user = str(input("Enter the Database User: "))
        db_pass = input("Enter the Database User Password: ")
        self.DBConMaker(self.db_name, db_user, self.db_host, db_pass)

    #Method for Calling the required method when Exception raising
    def DBErrorSolver(self, flag):
        {
            ErrorFlags.get('DBERR'): self.AskDBName,
            ErrorFlags.get('USRER'): self.AskUserCredit,
            ErrorFlags.get('HTERR'): self.AskHostName
        }.get(flag)()

    #Method For returning what type of Exception is raised
    def ErrorTypeSerializer(self, type):
        if ExceptionStr.get('AUTH') in type:
            return ErrorFlags.get('USRER')
        if ExceptionStr.get('DBEX') in type:
            return ErrorFlags.get('DBERR')
        if ExceptionStr.get('HTRANS') in type:
            return ErrorFlags.get('HTERR')
        if ExceptionStr.get('HCONER') in type:
            return ErrorFlags.get('HTERR')

    #Method for Making the Db Connection and return the connection object
    def DBConMaker(self, db_name=None, db_user=None, db_host=None, db_pass=None):
        try:
            self.db_conn = psql.connect(dbname=db_name,
                                       user=db_user,
                                       host=db_host,
                                       password=db_pass,
                                       port=5432)
            if self.db_conn != None:
                return True,self.db_conn
        except KeyboardInterrupt:
            quit()
        except psql.DatabaseError as ex:
            self.DBMessageCatcher(ex)
            self.DBErrorSolver(self.ErrorTypeSerializer(str(ex)))

    #Method for Making the DB Packup
    def DB_Backuper(self, db_filename,db_user, db_host, db_pass,tar_db):
        if not db_host:
            db_host='localhost'
        
        if not db_filename:
            db_filename = '/tmp/{tar_db}_db.sql'.format(tar_db=tar_db)

        command = f'pg_dump --host={db_host} ' \
            f'--dbname={tar_db} ' \
            f'--username={db_user} ' \
            f'--no-password ' \
            f'--format=p ' \
            f'--file=./SQL_DB_BACKUPS/{db_filename}_db.sql '

        try:
            proc = Popen(command, shell=True, env={
                'PGPASSWORD': db_pass
            })
            self.DBMessageCatcher("Please Wait The DB Backup process will going....")
            proc.wait()
            return True
        except:
            return False

    #Method for getting the DB Size in the target host
    def DB_SizeInspector(self, db_user=None, db_host=None, db_pass=None, tar_db=None):
        try:
            command = "PGPASSWORD={db_pass} psql -U {db_user} -h {db_host} -d {tar_db} -c {x}".format(
                x='"select pg_size_pretty(pg_database_size({z})) as size;"'.format(z="'{db_s}'".format(db_s=tar_db)), 
                db_pass=db_pass, db_user=db_user,db_host=db_host,tar_db=tar_db)
            result = run(command, stdout=PIPE, stderr=PIPE,
                    universal_newlines=False, shell=True)
            size = result.stdout.decode("utf-8").split('\n')[2]
            size_int = size.split(' ')[1]
            size_str = size.split(' ')[2]
            return True,size_int,size_str
        except:
            return False,False

    #Method for Getting the Dumped Sql file Size
    def DUMP_DB_Size(self, filepath):
        info = self.fileinfo.getFileInspector(filepath)
        if info[0]:
            filesize = self.fileinfo.getInfoParser("Size",info[1])
            if filesize[0]:
                size_db = self.fileinfo.getFileSize(filesize[1])
                self.DBMessageCatcher("Dumped SQL File size is: {x}".format(x=size_db))

    #Method for Creating the Needed DB in the database 
    def CREATE_DB(self,db_user,db_host,db_pass,new_db):
        try:
            command = "PGPASSWORD={db_pass} psql -U {db_user} -h {db_host} -c {create_db}".format(db_pass=db_pass,
                                                                                                  db_user=db_user,
                                                                                                  db_host=db_host,
                                                                                                  create_db="'create database {new_db} with owner {dbs_user};'".format(new_db=new_db,
                                                                                                                                                                      dbs_user=db_user))
            print(command)
            result = run(command, stdout=PIPE, stderr=PIPE,universal_newlines=False, shell=True)
            print(result)
            if result.returncode == 0:
                return True 
            else:
                return False 
        except:
            return False

    #Method for Restore the SQL DB to Created DB
    def RestoreSQL(self, db_user, db_host, db_pass, new_db,sql_file_name):
        sql_file_path = "./SQL_DB_BACKUPS/{x}".format(x=sql_file_name)
        try:
            command = "PGPASSWORD={db_pass} psql -U {db_user} -h {db_host} -d {db_name} < {sql_f}".format(db_pass=db_pass,db_user=db_user,
                                                                                                          db_host=db_host, db_name=new_db, sql_f=sql_file_path)
            result = run(command, stdout=PIPE, stderr=PIPE,
                         universal_newlines=False, shell=True)
            if result.returncode == 0:

                return True
            else:
                return False 
        except:
            return False
    


