from colorama import Fore, Style
import requests
import json
import pandas as pd
import time
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor


# config file
configFileName = "configuration.json"

# make a format for the list of dropdown to validate, field_id, options_from_excel, options_from_existing
def customFieldDropdownLabelFormat(importFile,existingFieldForTheList,customFieldDropdownLabel):
    dropDownLabelToValidate = {}
    # make sure that this is existing on the header
    try:
        # make sure we also do it in label via comma separated
        uniqueListFromExcelColumn = []
        for headerNameData in importFile[customFieldDropdownLabel['header_name_on_excel']].tolist():
            if str(headerNameData) != "nan":
                # separate by splitter
                uniqueListFromExcelColumn.extend(headerNameData.split(customFieldDropdownLabel["splitter"]))
                # strip the white spaces causes by splitter
                uniqueListFromExcelColumn = [item.strip() for item in uniqueListFromExcelColumn]
        # now get the existing custom field and all of it's value
        existingOptionFromCF = [item for item in existingFieldForTheList if item['id'] == customFieldDropdownLabel['cf_id']][0]
        print(f"{Fore.BLUE}###[CUSTOM FIELD DROPDOWN/LABEL FORMAT][FOUND]:{Style.RESET_ALL} We found {customFieldDropdownLabel['header_name_on_excel']} on both excel and your Workspace Custom Fields.")
        # set the data
        dropDownLabelToValidate = {
            'field':{
                # custom field ID from configuration
                'field_id': customFieldDropdownLabel['cf_id'],
                # get all the unique date from a column specifically, this will act as a checker for the menu
                'options': list(set(uniqueListFromExcelColumn)),
                # separate custom field as existing data
                'existing_from_cf': existingOptionFromCF,
                'found_in_header':True
            }
        }
    except:
        # push only the ID
        dropDownLabelToValidate = {
        'field':{
                # custom field ID from configuration
                'field_id': customFieldDropdownLabel['cf_id'],
                'found_in_header':False
            }
        }
        # set a log
        print(f"{Fore.LIGHTYELLOW_EX}###[CUSTOM FIELD DROPDOWN/LABEL FORMAT][NOT FOUND]:It seems like the {str(customFieldDropdownLabel['header_name_on_excel'])} is not existing on the excel as a header. We're gonna CFID: {str(customFieldDropdownLabel['cf_id'])} as EMPTY.{Style.RESET_ALL}")
        pass

    return dropDownLabelToValidate

# function that validates a custom field menu based on the excel column that it is mapped into; we creating a new list ^ that is unique so we can identify it
def validateCustomFieldsDropDownLabelfromExcelColumn(cfFieldDropownLabel):    
    # we only making it true if we are clear on every custom field menu
    isValid = True
    for cfField in cfFieldDropownLabel:
        # this means the cfDropdown is not found in the header we will moved and set it
        if cfField['field']['found_in_header']:
            
            availableOptions = cfField['field']['existing_from_cf']['type_config']['options']
            existingOptionsonExcel = cfField['field']['options']
            # check the custom field type either we get label or name
            fieldTypeKey = "label" if cfField['field']['existing_from_cf']['type'] == "labels" else "name"
            # find and match
            for existingOption in existingOptionsonExcel:
                validateExistingOption = [item for item in availableOptions if item[fieldTypeKey] == str(existingOption)]
                if not validateExistingOption:
                    print(f"{Fore.RED}[{str(existingOption)}][NOT FOUND]:{Style.RESET_ALL} Is not existing on the menu option for custom field id {str(cfField['field']['existing_from_cf']['id'])} - {str(cfField['field']['existing_from_cf']['name'])}.")
                    isValid = False
                else:
                    print(f"{Fore.GREEN}[{str(existingOption)}][EXIST]:{Style.RESET_ALL} We found this exist! {str(cfField['field']['existing_from_cf']['id'])} - {str(cfField['field']['existing_from_cf']['name'])}")

    return isValid

def validateURL(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

# check the format here https://www.tutorialspoint.com/python/time_strptime.htm when going to pd it always return this pattern %Y-%m-%d %H:%M:%S
def validateDate(dateString,pattern = "%Y-%m-%d %H:%M:%S"):
    try:
        time.strptime(str(dateString), pattern)
        return True
    except:
        return False

def createTask(url,taskCreatePayload,listID,apiKey): 
    try:
        # post request
        taskPostResponse = requests.post(url + "list/" + listID  + "/task", data=json.dumps(taskCreatePayload),headers={'content-type' : 'application/json', "authorization" : apiKey})
        # response
        return taskPostResponse
    except requests.exceptions.RequestException as e:
        return e

# start importing here per LIST
def importExceltoList(fileListMapping,importType,testLimit,listOfTags,taskStatus,url,apiKey,configFile):
     # read the excel and skip the 1st header
    importFile = pd.read_excel(fileListMapping['file_path'])

    # get the existing field specifically on the list
    existingFieldForTheList = requests.get(url + "list/" + fileListMapping['list_id']  + "/field",headers={'content-type' : 'application/json', "authorization" : apiKey})
    existingFieldForTheList = existingFieldForTheList.json()['fields']

    # set the dropdown for validate
    listofCustomFieldDropdownLabeltoValidate = []
    # custom field drop down from configuration
    for customFieldDropDownLabel in configFile['custom_field_dropdown_label']:
        # this is where we pull out all the existing custom field value and compare on the column that we have
        listofCustomFieldDropdownLabeltoValidate.append(customFieldDropdownLabelFormat(importFile,existingFieldForTheList,customFieldDropDownLabel))

    # validate first before move to importing, this goes only one time so you can clear this out probably to reduce the process (but not a big deal)
    if (validateCustomFieldsDropDownLabelfromExcelColumn(listofCustomFieldDropdownLabeltoValidate)):
        # FOR TEST RUN ONLY + 1 since we start with ZERO
        TESTING_LIMIT_PER_SHEET = testLimit

        # start time per list
        startTime = time.time()
        # set a count for success for each sheet
        successCount = 0
        # set a file name
        fileLogName = str(importType + "_log_" + fileListMapping['file_name'].replace(" ", "_").lower() + ("" if fileListMapping['continue'] is not True else "_continue") + ".txt")
        ## set a log file
        logFile = open ("import-logs/" + fileLogName, "w")

        print(f"{Fore.BLUE}### VALIDATION COMPLETE!!\n### MIGRATION STARTS HERE\n### MIGRATION TYPE: {Style.RESET_ALL}{str(importType)}\n{Fore.BLUE}### FILE NAME: {Style.RESET_ALL}{str(fileListMapping['file_name'])}\n{Fore.BLUE}### FILE LOG NAME: {Style.RESET_ALL}{fileLogName}")
        logFile.write(f"### VALIDATION COMPLETE!!\n### MIGRATION STARTS HERE\n### MIGRATION TYPE: {str(importType)}\n### FILE NAME: {str(fileListMapping['file_name'])}\n### FILE LOG NAME: {fileLogName}\n\n")

        # arrange array here based on the arrangement on the sample task #2r0unr9 !!!
        descriptionArrangement = [
                                    {
                                        #summary
                                        "title":"Summary",
                                        "list":['Summary']
                                    },
                                    {
                                        #job ticket
                                        "title":"Job Ticket",
                                        "list":[
                                                    'Job_Type',
                                                    'Quantity',
                                                    'Delivery_Info',
                                                    'Production_Specs',
                                                    'Budget_Notes',
                                                    'Reference_Job',
                                                    "Billing Status",
                                                    'Customer Code',
                                                    'Bill To',
                                                    'Legacy_Activity_Code',
                                                    'Legacy_Campus',
                                                    'Legacy_Id',
                                                    'Legacy_Job_Number',
                                                    'Legacy_Org_Code',
                                                    'Communications_Manager',
                                                    'Copy_Editor',
                                                    'Designer',
                                                    'Production_Lead',
                                                    'Writer_Editor',
                                                    'Location',
                                                    'Partner',
                                                    'Partner_Contact_Info',
                                                    'Partner_Department',
                                                    'jobCategory.Name',
                                                    'jobGroup.Name',
                                                    'jobLevel.Name'
                                                ]
                                    },
                                    {
                                        #Invoice Data
                                        "title":"Invoice Data",
                                        "list":[
                                                    'budgetInfo.Vendor',
                                                    'budgetInfo.Account_Number',
                                                    'budgetInfo.Actual_Expense',
                                                    'budgetInfo.Budget_Account_Type',
                                                    'budgetInfo.Budget_Activity_Code',
                                                    'budgetInfo.Budget_Location_Code',
                                                    'budgetInfo.Budget_Org_Code',
                                                    'budgetInfo.Invoice_Date',
                                                    'budgetInfo.Invoice_Number',
                                                    'budgetInfo.Job_Number',
                                                    'budgetInfo.SCAD_PO_Number',
                                                ]
                                    },
                                    {
                                        #Job Notes
                                        "title":"Job Notes",
                                        "list":[
                                                    'Discussion_Notes.note'
                                                ]
                                    },
                                ]

        # now loop
        for row in range(fileListMapping['range_start'],len(importFile)):
            # THIS IS WHERE WE START TO MODIFY THE PAYLOAD!

            # set payload for all  custom field!!!
            cfPayload = []

            # format custom field dropdown!!!
            for cFieldDD in configFile['custom_field_dropdown_label']:
                cFieldDDValue = ""
                cFieldLabelValue = []
                # # make sure we found it on header else make it empty, set the matching from config file
                cfDropdownList = [item for item in listofCustomFieldDropdownLabeltoValidate if str(item['field']['field_id']) == str(cFieldDD['cf_id'])][0]
                if cfDropdownList['field']['found_in_header']:
                    # for labels Type
                    if cfDropdownList['field']['existing_from_cf']['type'] == "labels" and not pd.isna(importFile[cFieldDD['header_name_on_excel']][row]):
                        for label in importFile[cFieldDD['header_name_on_excel']][row].split(cFieldDD["splitter"]):
                            # get the value and push it
                            cFieldLabelValue.append("" if pd.isna(label.strip()) else [item for item in cfDropdownList['field']['existing_from_cf']['type_config']['options'] if item['label'] == str(label.strip())][0]['id'])              
                    
                    # for drop_down Type
                    if cfDropdownList['field']['existing_from_cf']['type'] == "drop_down":
                        cFieldDDValue = "" if pd.isna(importFile[cFieldDD['header_name_on_excel']][row]) else [item for item in cfDropdownList['field']['existing_from_cf']['type_config']['options'] if item['name'] == str(importFile[cFieldDD['header_name_on_excel']][row])][0]['orderindex']
                else:
                    logFile.write(f"[ROW #{str(row)}]: It seems like the {str(cFieldDD['header_name_on_excel'])} is not existing on the excel as a header. We're gonna CFID:{str(cFieldDD['cf_id'])} as EMPTY.\n")
                    pass
                
                # for drop_down getting error {'err': 'Value must be an option index or uuid', 'ECODE': 'FIELD_011'} to prevent just don't push as part of the payload
                if cFieldDDValue:
                     # push
                    cfPayload.append({"id": str(cFieldDD['cf_id']), "value": cFieldDDValue})
            
                # for label getting error {'err': 'Value must be an option index or uuid', 'ECODE': 'FIELD_011'} to prevent just don't push as part of the payload
                if cFieldLabelValue:
                     # push
                    cfPayload.append({"id": str(cFieldDD['cf_id']), "value": cFieldLabelValue})

            # format custom field link!!!
            for cField in configFile['custom_field_link']:
                    try:
                        cfValue =  "" if pd.isna(importFile[cField['header_name_on_excel']][row]) or not validateURL(importFile[cField['header_name_on_excel']][row]) else str(importFile[cField['header_name_on_excel']][row])
                        # getting error {'err': 'Value is not a valid URL', 'ECODE': 'FIELD_010'} to prevent just don't push as part of the payload
                        if cfValue:
                            cfPayload.append({"id": str(cField['cf_id']), "value":  cfValue})
                    except:
                        #if we seems not to found it on the column just set the value to empty
                        logFile.write(f"[ROW #{str(row)}]: It seems like the {str(cField['header_name_on_excel'])} is not existing on the excel as a header. We're gonna CFID:{str(cField['cf_id'])} as EMPTY.\n")
                        pass

            # format custom field date!!!
            for cField in configFile['custom_field_date']:
                    try:
                        cfValue =  "" if pd.isna(importFile[cField['header_name_on_excel']][row]) or not validateDate(importFile[cField['header_name_on_excel']][row]) else int(importFile[cField['header_name_on_excel']][row].timestamp() * 1000)
                        #getting error {'err': 'Value is not a valid date', 'ECODE': 'FIELD_017'} to prevent just don't push as part of the payload
                        if cfValue:
                            cfPayload.append({"id": str(cField['cf_id']), "value":  cfValue})
                    except:
                        #if we seems not to found it on the column just set the value to empty
                        logFile.write(f"[ROW #{str(row)}]: It seems like the {str(cField['header_name_on_excel'])} is not existing on the excel as a header. We're gonna CFID:{str(cField['cf_id'])} as EMPTY.\n")
                        pass

            # format description!!!
            description = ""
            for fieldCol in descriptionArrangement:
                checkSubListDescription = False
                descriptionGroup = ""
                descriptionGroup += "" if fieldCol['title'] == "Summary" else str("---\n") 
                descriptionGroup += "" if fieldCol['title'] == "Summary" else str("# " + fieldCol['title']) + "\n\n"
                for fieldContent in fieldCol['list']:
                    try:
                        # this flag make sure in each sub we have content if it changes to True it means we have else it will just stay false
                        if not pd.isna(importFile[fieldContent][row]): checkSubListDescription = True
                        descriptionGroup += str("**" + fieldContent.replace("_", " ").replace(".", " ").title()) + ":** " 
                        descriptionGroup += "N/A \n\n" if pd.isna(importFile[fieldContent][row]) else str(importFile[fieldContent][row]).strip() + "\n\n"
                    except:
                        logFile.write(f"[ROW #{str(row)}]: It seems like the {str(fieldContent)} is not existing on the excel as a header. Ignoring from adding it on the DESCRIPTION. \n")
                        pass
                    
                # do we have any atleast one value?
                if checkSubListDescription:
                    # push the data
                    description += descriptionGroup
                else:
                    logFile.write(f"[ROW #{str(row)}]: It seems like the {str(fieldCol['title'])} Section for description data all empty. Ignoring from adding it on the DESCRIPTION. \n")

            # format task name!!!
            taskName = "#" + str(importFile["Job #"][row]) + " - " + str(importFile["Job Name"][row])

            # due date !!!
            dueDate = 0 if pd.isna(importFile["Due Date"][row]) or not validateDate(importFile["Due Date"][row]) else int(importFile["Due Date"][row].timestamp() * 1000)

            # set the payload
            taskCreatePayload = {
                "name": str(taskName),
                "markdown_description": str(description),
                "due_date": dueDate,
                "custom_fields": cfPayload,
                "tags": listOfTags,
                "status":taskStatus
            }

            # post request
            taskPostResponse = createTask(url,taskCreatePayload,fileListMapping['list_id'],apiKey)

            # confirm
            jsonResponse = taskPostResponse.json()

            if taskPostResponse.status_code == 200:
                successCount += 1
                print(f"{Fore.GREEN}[LIST ID {fileListMapping['list_id']}][ROW #{str(row)}][IMPORT SUCCESS]:{Style.RESET_ALL} {str(taskName)}. [TASK ID]: {str(jsonResponse['id'])}")
                logFile.write(f"[ROW #{str(row)}][IMPORT SUCCESS]: {str(taskName)}. [TASK ID]: {str(jsonResponse['id'])}\n\n")
            else:
                print(f"{Fore.RED}[LIST ID {fileListMapping['list_id']}][ROW #{str(row)}][IMPORT FAILED]:{Style.RESET_ALL} {str(taskName)}.")
                print(taskCreatePayload)
                print(taskPostResponse.json())
                logFile.write(f"[ROW #{str(row)}][IMPORT FAILED]: {str(taskName)}.\n{str(jsonResponse)}\n[PAYLOAD]: {str(taskCreatePayload)}\n[RESPONSE]: {str(taskPostResponse.json())}\n\n")

            # FOR TEST RUN ONLY
            if row >= TESTING_LIMIT_PER_SHEET and importType == "TEST" : break

        print(f"{Fore.GREEN}### IMPORT COMPLETE!! TOTAL OF {str(successCount)} out of {str(len(importFile))}{Style.RESET_ALL}")
        print("--- %s seconds ---" % (time.time() - startTime))
        logFile.write(f"\n### IMPORT COMPLETE!! TOTAL OF {str(successCount)} out of {str(len(importFile))}\n")
        logFile.write("--- %s seconds ---" % (time.time() - startTime))
        # close log file for LIST
        logFile.close()

def batchImport():
    # get configuration
    with open(configFileName) as f:
        configFile = json.load(f)

    # configuration PS: range_start should be + 2 + header and start with 0
    url = configFile["base_url"]
    apiKey = configFile["api_key"]
    importType = configFile["import_type"] # TEST OR LIVE
    testLimit = configFile["test_limit"] # FOR TEST RUN ONLY + 1 since we start with ZERO will IGNORE IF TYPE == LIVE
    listOfTags = configFile["list_of_tags"] # TAGS
    taskStatus = configFile["task_status"] # STATUS

    #start threading
    with ThreadPoolExecutor(max_workers=20) as executor:
        for fileListMapping in configFile['file_list_mapping']:
            if fileListMapping['status'] == "for_import":
                executor.submit(importExceltoList, fileListMapping, importType, testLimit, listOfTags, taskStatus, url, apiKey, configFile)
# init           
batchImport()