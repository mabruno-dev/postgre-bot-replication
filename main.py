import replication
import schedule
from time import sleep
from creatingJson import *
import os

def syncronizeData():
    path = "database.json"
    if not os.path.exists(path):
        savingJson()

    local = replication.reading_local('local')
    server = replication.reading_local('server')
    if local:
        replication.copyingToDestiny('server',local)
    if server:
        replication.copyingToDestiny('local',server)

def main():
    schedule.every(1).seconds.do(syncronizeData)
    while True:
        schedule.run_pending()
        sleep(0.5)

main()
