import mysql.connector
import re
import os
import sys
import logging
from datetime import datetime

sql_dir_path = sys.argv[1]
username = sys.argv[2]
hostname = sys.argv[3]
dbname = sys.argv[4]
password = sys.argv[5]

cnx = mysql.connector.connect(
    host=hostname,
    user=username,
    passwd=password,
    database=dbname
    )

mycursor = cnx.cursor()

#Getting database version number from versionTable 
def currentversion_versionTable():
    mycursor.execute("SELECT version from versionTable")
    version = [int(item[0]) for item in mycursor.fetchall()][0]
    return version

#Function to execute commands in the file
def executeScriptsFromFile(file):
    fd = open(file, 'r')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')

    for command in sqlCommands:
        try:
            if command.strip() != '':
                mycursor.execute(command)
                logging.info('Executed commands in ' + file)
        except IOError, msg:
            print "Command skipped: ", msg

def get_version_from_filename(filename):
    return re.search(r'\d+', filename).group(0)

def db_upgrade_exec_scripts():
    dtRun_hhmmss = datetime.now().strftime("%y-%m-%d_%H%M%S")
    logfile = 'logfile_' + dtRun_hhmmss + '.log'
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info(datetime.now().strftime("%y-%m-%d_%H%M%S") + ' Script started')

    logging.info('Reading scripts from:' + sql_dir_path)
    
    #Sorted list of files present in sqlscripts directory
    sorted_list_of_files = sorted(os.listdir(sql_dir_path))
    logging.info('Sorted list of sql files: ' + str(sorted_list_of_files)) 
    max_file = int(sorted(sorted_list_of_files)[-1][0:3])
    
    version = currentversion_versionTable()
    logging.info('Database connection established successfully')
    print("Current version of database: " + str(version))
    logging.info('Current version of database is ' + str(version))

    if max_file <= version:
        print ("version number of the database is greater than the highest file number from the scripts, so nothing is executed")
        logging.warning('version number of the database is greater than the highest file number from the scripts, so nothing is executed')
    else:
        for filename in sorted_list_of_files:
            version_from_filename = get_version_from_filename(filename)
            if int(version_from_filename) > version:
                file_path = sql_dir_path + '/' + filename
                logging.info('version of database is less than file ' + filename + ' so executing the file and updating the database version')
                #Executing commands in the file
                executeScriptsFromFile(file_path)
                cnx.commit()
                #Updating Version in versionTable
                newversion = int(version_from_filename)
                mycursor.execute("update versionTable set version = %s" % (newversion))
                cnx.commit()
                #Getting updated version from versionTable
                version = currentversion_versionTable()
                logging.info('Updated version of database is: ' + str(version))
        print("Updated version of database is: " +  str(version))
    print("For more details please see the logfile: " + logfile) 

if __name__=='__main__':
    db_upgrade_exec_scripts()

mycursor.close()
cnx.close()
logging.info('Database connection closed')
logging.info(datetime.now().strftime("%y-%m-%d_%H%M%S") + ' Script Completed!!')

