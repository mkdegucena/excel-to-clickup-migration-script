import requests
import json
import pandas as pd

# make a format for the list of dropdown to validate, field_id, options_from_excel, options_from_existing
def customFieldDropdownFormat(customFieldDropdown):

    dropDownToValidate = {}
    # make sure that this is existing on the header
    try:
        uniqueListFromExcelColumn = [x for x in list(set(importFile[customFieldDropdown['header_name_on_excel']].tolist())) if str(x) != "nan"]
        existingOptionFromCF = [item for item in existingFieldForTheList if item['id'] == customFieldDropdown['cf_id']][0]
        dropDownToValidate = {
            'field':{
                # custom field ID from configuration
                'field_id': customFieldDropdown['cf_id'],
                # get all the unique date from a column specifically, this will act as a checker for the menu
                'options': uniqueListFromExcelColumn,
                # separate custom field as existing data
                'existing_from_cf': existingOptionFromCF,
                'found_in_header':True
            }
        }
    except:
        # push only the ID
        dropDownToValidate = {
            'field':{
                # custom field ID from configuration
                'field_id': customFieldDropdown['cf_id'],
                'found_in_header':False
            }
        }
        # set a log
        print("It seems like the " + str(customFieldDropdown['header_name_on_excel']) + " is not existing on the excel as a header. We're gonna CFID:" + str(customFieldDropdown['cf_id'] + " as EMPTY."))
        pass

    return dropDownToValidate

# function that validates a custom field menu based on the excel column that it is mapped into; we creating a new list ^ that is unique so we can identify it
def validateCustomFieldsDropDownfromExcelColumn(cfFieldDropown):

    # we only making it true if we are clear on every custom field menu
    isValid = True
    for cfField in cfFieldDropown:
        # this means the cfDropdown is not found in the header we will moved and set it
        if cfField['field']['found_in_header']:
            availableOptions = cfField['field']['existing_from_cf']['type_config']['options']
            existingOptionsonExcel = cfField['field']['options']
            for existingOption in existingOptionsonExcel:
                validateExistingOption = [item for item in availableOptions if item['name'] == str(existingOption)]
                if not validateExistingOption:
                    print("[" + str(existingOption) + "] is not existing on the menu option for custom field id " + str(cfField['field']['existing_from_cf']['id']) + " - " + str(cfField['field']['existing_from_cf']['name']))
                    isValid = False

    return isValid

# get configuration
with open("configuration.json") as f:
    configFile = json.load(f)

# configuration
url = configFile["base_url"]
apiKey = configFile["api_key"]

for fileListMapping in configFile['file_list_mapping']:

    # read the excel and skip the 1st header
    importFile = pd.read_excel(fileListMapping['file_name'])

    # get the existing field specifically on the list
    existingFieldForTheList = requests.get(url + "list/" + fileListMapping['list_id']  + "/field",headers={'content-type' : 'application/json', "authorization" : apiKey})
    existingFieldForTheList = existingFieldForTheList.json()['fields']

    # set the dropdown for validate
    listofCustomFieldDropdowntoValidate = []
    # custom field drop down from configuration
    for customFieldDropDown in configFile['custom_field_dropdown']:
        # this is where we pull out all the existing custom field value and compare on the column that we have
        listofCustomFieldDropdowntoValidate.append(customFieldDropdownFormat(customFieldDropDown))

    ######## IMPORTING START HERE #########

    # validate first before move to importing, this goes only one time so you can clear this out probably to reduce the process (but not a big deal)
    if (validateCustomFieldsDropDownfromExcelColumn(listofCustomFieldDropdowntoValidate)):
        
        # FOR TEST RUN ONLY + 1 since we start with ZERO
        TESTING_LIMIT_PER_SHEET = 0

        ## set a log file
        logFile = open ("import-logs/" + str("log_" + fileListMapping['file_name'].replace(" ", "_").lower().replace("xls","txt")), "w")

        print("### VALIDATION COMPLETE!!")
        print("### MIGRATION STARTS HERE")
        print("### FILE NAME:" + fileListMapping['file_name'])
        print("### FILE LOG NAME:" + str("log_" + fileListMapping['file_name'].replace(" ", "_").lower()))
        logFile.write("### VALIDATION COMPLETE!!\n### MIGRATION STARTS HERE\n### FILE NAME:" + str(fileListMapping['file_name']) + "\n### FILE LOG NAME:" + str("log_" + fileListMapping['file_name'].replace(" ", "_").lower()) + "\n\n")
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
                                                    'Legacy_Org_Code'
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

        # set a count for success for each sheet
        successCount = 0

        # now loop
        for row in range(0,len(importFile)):

            # THIS IS WHERE WE START TO MODIFY THE PAYLOAD!

            # set payload for custom field!!!
            cFieldDDPayload = []

            # format custom field dropdown!!!
            for cFieldDD in configFile['custom_field_dropdown']:
                cFieldDDIndex = ""
                # make sure we found it on header else make it empty, set the matching from config file
                cfDropdownList = [item for item in listofCustomFieldDropdowntoValidate if str(item['field']['field_id']) == str(cFieldDD['cf_id'])][0]
                if cfDropdownList['field']['found_in_header']:
                    cFieldDDIndex = "" if pd.isna(importFile[cFieldDD['header_name_on_excel']][row]) else [item for item in cfDropdownList['field']['existing_from_cf']['type_config']['options'] if item['name'] == str(importFile[cFieldDD['header_name_on_excel']][row])][0]['orderindex']
                else:
                    print("[ROW #"+ str(row) + "]: It seems like the " + str(cFieldDD['header_name_on_excel']) + " is not existing on the excel as a header. We're gonna CFID:" + str(cFieldDD['cf_id'] + " as EMPTY."))
                    logFile.write("[ROW #"+ str(row) + "]: It seems like the " + str(cFieldDD['header_name_on_excel']) + " is not existing on the excel as a header. We're gonna CFID:" + str(cFieldDD['cf_id'] + " as EMPTY. \n"))
                # push
                cFieldDDPayload.append({"id": str(cFieldDD['cf_id']), "value":  cFieldDDIndex})
            

            # format custom field!!!
            for cField in configFile['custom_field']:
                    try:
                        cfValue =  "" if pd.isna(importFile[cField['header_name_on_excel']][row]) else str(importFile[cField['header_name_on_excel']][row])
                        cFieldDDPayload.append({"id": str(cField['cf_id']), "value":  cfValue})
                    except:
                        #if we seems not to found it on the column just set the value to empty
                        print("[ROW #"+ str(row) + "]: It seems like the " + str(cField['header_name_on_excel']) + " is not existing on the excel as a header. We're gonna CFID:" + str(cField['cf_id'] + " as EMPTY."))
                        logFile.write("[ROW #"+ str(row) + "]: It seems like the " + str(cField['header_name_on_excel']) + " is not existing on the excel as a header. We're gonna CFID:" + str(cField['cf_id'] + " as EMPTY. \n"))
                        cFieldDDPayload.append({"id": str(cField['cf_id']), "value":  ""})
                        pass

            # format description!!!
            description = ""
            for fieldCol in descriptionArrangement:
                description += "" if fieldCol['title'] == "Summary" else str("---\n") 
                description += "" if fieldCol['title'] == "Summary" else str("# " + fieldCol['title']) + "\n\n"
                for fieldContent in fieldCol['list']:
                    try:
                        description += str("**" + fieldContent.replace("_", " ").replace("."," ").title()) + ":** " 
                        description += "N/A \n\n" if pd.isna(importFile[fieldContent][row]) else str(importFile[fieldContent][row]).strip() + "\n\n"  
                    except:
                        print("[ROW #"+ str(row) + "]: It seems like the " + str(fieldContent) + " is not existing on the excel as a header. Ignoring from adding it on the DESCRIPTION.")
                        logFile.write("[ROW #"+ str(row) + "]: It seems like the " + str(fieldContent) + " is not existing on the excel as a header. Ignoring from adding it on the DESCRIPTION. \n")
                        pass

            # format task name!!!
            taskName = "#" + str(importFile["Job #"][row]) + " - " + str(importFile["Job Name"][row])

            # set the payload
            taskCreatePayload = {
                "name": str(taskName),
                "markdown_description": str(description),
                "due_date": int(importFile["Due Date"][row].timestamp() * 1000),
                "custom_fields": cFieldDDPayload,
                "tags": ["script-testing"],
                "status":"CLOSED"
            }

            # post request
            taskPostResponse = requests.post(url + "list/" + fileListMapping['list_id']  + "/task", data=json.dumps(taskCreatePayload),headers={'content-type' : 'application/json', "authorization" : apiKey})
            # confirm
            if taskPostResponse.status_code == 200:
                successCount += 1
                jsonResponse = taskPostResponse.json()
                print("[ROW #"+ str(row) + "][IMPORT SUCCESS]: " + str(taskName) + ". [TASK ID]: " + str(jsonResponse['id']))
                logFile.write("[ROW #"+ str(row) + "][IMPORT SUCCESS]: " + str(taskName) + ". [TASK ID]: " + str(jsonResponse['id']) + "\n")
            else:
                print("[ROW #"+ str(row) + "][IMPORT FAILED]: " + str(taskName) + ".")
                logFile.write("[ROW #"+ str(row) + "][IMPORT FAILED]: " + str(taskName) + ". \n")
                # additional log for investigation
                print(taskPostResponse)

            # FOR TEST RUN ONLY
            if row >= TESTING_LIMIT_PER_SHEET : break


        print("### IMPORT COMPLETE!! TOTAL OF " + str(successCount) + " out of " + str(len(importFile)))
        logFile.write("\n### IMPORT COMPLETE!! TOTAL OF " + str(successCount) + " out of " + str(len(importFile)))
        ## close the file
        logFile.close()