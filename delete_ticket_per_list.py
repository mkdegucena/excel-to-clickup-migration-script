from colorama import Fore, Style
import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time

# config file
configFileName = "configuration.json"

def getTasksPerList(url,listID,page,apiKey): 
    getTasks = requests.get(url + "list/" + listID  + "/task?page=" + str(page) + "&include_closed=true",headers={'content-type' : 'application/json', "authorization" : apiKey})
    return getTasks

def deleteTask(url,taskID,apiKey):
    try:
        requests.delete(url + "task/" + taskID,headers={'content-type' : 'application/json', "authorization" : apiKey})
        print(f"{Fore.GREEN}[TASK ID {taskID}]: DELETE SUCCESS{Style.RESET_ALL}")
    except requests.exceptions.RequestException as e:
        print(f"{Fore.RED}[TASK ID {taskID}]: DELETE FAILED{Style.RESET_ALL}")
        print(e.json())

def deleteTasksPerList(url, fileListMapping, apiKey):  
    page = 0
    lastPage = False

    while not lastPage:
        print(f"{Fore.BLUE}[PAGE]: #{page}{Style.RESET_ALL}")
        # get all the task per list
        listOfTask = getTasksPerList(url,fileListMapping['list_id'],page,apiKey)
        # delete per page
        with ThreadPoolExecutor(max_workers=20) as executor:
            for task in listOfTask.json()['tasks']: 
                executor.submit(deleteTask, url, task['id'], apiKey)
        # add page
        page = page + 1
        # is this the last page?
        if len(listOfTask.json()['tasks']) < 100:
            lastPage = True

def batchDeleteTasksPerList():
    # get configuration
    with open(configFileName) as f:
        configFile = json.load(f)

    # configuration PS: range_start should be + 2 + header and start with 0
    url = configFile["base_url"]
    apiKey = configFile["api_key"]

    #start threading
    with ThreadPoolExecutor(max_workers=20) as executor:
        for fileListMapping in configFile['file_list_mapping']:
            if fileListMapping['status'] == "for_task_delete":
                executor.submit(deleteTasksPerList,url, fileListMapping, apiKey)
# init           
batchDeleteTasksPerList()