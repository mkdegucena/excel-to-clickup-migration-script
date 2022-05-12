import requests
import json
import pandas as pd
from pprint import pprint


# make a format for the list of dropdown to validate, field_id, options_from_excel, options_from_existing
def dropdownFormat(dropDownField):

    dropDownToValidate = {}
    uniqueListFromExcelColumn = [x for x in list(set(importFile[dropDownField['header_name_on_excel']].tolist())) if str(x) != "nan"]
    existingOptionFromCF = [item for item in existingFieldForTheList if item['id'] == dropDownField['cf_id']][0]
    dropDownToValidate = {
        'field':{
            # custom field ID from configuration
            'field_id': dropDownField['cf_id'],
            # get all the unique date from a column specifically, this will act as a checker for the menu
            'options': uniqueListFromExcelColumn,
            # separate custom field as existing data
            'existing_from_cf': existingOptionFromCF
        }
    }

    return dropDownToValidate

# function that validates a custom field menu based on the excel column that it is mapped into; we creating a new list ^ that is unique so we can identify it
def validateCustomFieldsDropDownfromExcelColumn(cfFieldDropown):

    # we only making it true if we are clear on every custom field menu
    isValid = True
    for cfField in cfFieldDropown:
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
fileName = configFile["file_name"]
listID = configFile["list_id"]

# read the excel and skip the 1st header
importFile = pd.read_excel(fileName)

# set the header format for easy read and easy map
importFile.columns = importFile.columns.str.lower()
importFile.columns = importFile.columns.map(
    lambda x: x.replace("-", "_").replace(".", "_").replace(" ", "_")
)

# get the existing field specifically on the list
existingFieldForTheList = requests.get(url + "list/" + listID  + "/field",headers={'content-type' : 'application/json', "authorization" : apiKey})
existingFieldForTheList = existingFieldForTheList.json()['fields']

# set the dropdown for validate
listofDropdowntoValidate = []
# custom field drop down from configuration
for dropDownField in configFile['custom_field_dropdown']:
    listofDropdowntoValidate.append(dropdownFormat(dropDownField))

######### IMPORTING START HERE #########

# validate first before move to importing, this goes only one time so you can clear this out probably to reduce the process (but not a big deal)
if (validateCustomFieldsDropDownfromExcelColumn(listofDropdowntoValidate)):

    # for test run only comment if not using
    # limit = 1

    print("######### VALIDATION COMPLETE!! #########")
    print("######### MIGRATION STARTS HERE #########")

    # items to be removed for DESCRIPTION ONLY
    forDescriptionListIndex = [ele for ele in importFile.columns.tolist() if ele not in {
                                    # task name, default field and custom field
                                    "job_#", 
                                    "job_name",
                                    "due_date",
                                    "org_code",
                                    "activity_code",
                                    # remove permanently per import as per SCAD SUGGEST
                                    "customer_name",
                                    "legacy_start_date",
                                    "status",
                                    "jobcategory_name",
                                    "joblevel_name",
                                    "jobtype_name",
                                    "additionalcampuses_customer_code"
                                }]
    # arrange array here based on the arrangement on the sample task #2r0unr9
    descriptionArrangement = [
                                {
                                    #summary
                                    "title":"Summary",
                                    "list":['summary']
                                },
                                {
                                    #job ticket
                                    "title":"Job Ticket",
                                    "list":[
                                                'job_type',
                                                'quantity',
                                                'production_specs',
                                                'budget_notes',
                                                'reference_job',
                                                "billing_status",
                                                'customer_code',
                                                'bill_to',
                                                'legacy_activity_code',
                                                'legacy_campus',
                                                'legacy_id',
                                                'legacy_job_number',
                                                'legacy_org_code'
                                            ]
                                },
                                {
                                    #Invoice Data
                                    "title":"Invoice Data",
                                    "list":[
                                                'budgetinfo_vendor',
                                                'budgetinfo_account_number',
                                                'budgetinfo_actual_expense',
                                                'budgetinfo_budget_account_type',
                                                'budgetinfo_budget_activity_code',
                                                'budgetinfo_budget_location_code',
                                                'budgetinfo_budget_org_code',
                                                'budgetinfo_invoice_date',
                                                'budgetinfo_invoice_number',
                                                'budgetinfo_job_number',
                                                'budgetinfo_scad_po_number',
                                            ]
                                },
                                 {
                                    #Job Notes
                                    "title":"Job Notes",
                                    "list":[
                                                'discussion_notes_note'
                                            ]
                                },
                            ]

    # now loop
    for index in range(0,len(importFile)):

        # THIS IS WHERE WE START TO MODIFY THE PAYLOAD!

        # format description!
        description = ""
        for fieldCol in descriptionArrangement:
            description += "" if fieldCol['title'] == "Summary" else str("---\n") 
            description += "" if fieldCol['title'] == "Summary" else str("# " + fieldCol['title']) + "\n\n"
            for fieldContent in fieldCol['list']:
                description += str("**" + fieldContent.replace("_", " ").title()) + ":** " 
                description += "N/A \n\n" if pd.isna(importFile[fieldContent][index]) else str(importFile[fieldContent][index]).strip() + "\n\n"

        # format custom dropdown assignee!
        cFieldDDPayload = []
        for cFieldDD in configFile['custom_field_dropdown']:
            ddList = [item for item in listofDropdowntoValidate if str(item['field']['existing_from_cf']['id']) == str(cFieldDD['cf_id'])][0]
            cFieldDDIndex = "" if pd.isna(importFile[cFieldDD['header_name_on_excel']][index]) else [item for item in ddList['field']['existing_from_cf']['type_config']['options'] if item['name'] == str(importFile[cFieldDD['header_name_on_excel']][index])][0]['orderindex']
            cFieldDDPayload.append({"id": str(cFieldDD['cf_id']), "value":  cFieldDDIndex})

        # set the payload
        taskCreatePayload = {
            "name": "#" + str(importFile["job_#"][index]) + " - " + str(importFile["job_name"][index]),
            "markdown_description": str(description),
            "due_date": int(importFile["due_date"][index].timestamp() * 1000),
            "custom_fields": cFieldDDPayload,
            "tags": ["02-import-trial"]
        }

        # post request
        taskPostResponse = requests.post(url + "list/" + listID  + "/task", data=json.dumps(taskCreatePayload),headers={'content-type' : 'application/json', "authorization" : apiKey})
        # confirm
        if taskPostResponse.status_code == 200:
            print("[IMPORT SUCCESS]: " + "#" + str(importFile["job_#"][index]) + " - " + str(importFile["job_name"][index]))
        else:
            print("[IMPORT FAILED]: " + "#" + str(importFile["job_#"][index]) + " - " + str(importFile["job_name"][index]))
            # additional log for investigation
            print(taskPostResponse)

        # for test run only comment if not using
        # if index <= limit : break

print("[IMPORT COMPLETE]: Imported for a total of " + str(len(importFile)))