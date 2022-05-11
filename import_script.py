import requests
import json
import pandas as pd

# get configuration
with open("configuration.json") as f:
    data = json.load(f)

# configuration
url = data["base_url"]
apiKey = data["api_key"]
fileName = data["file_name"]
listID = data["list_id"]

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

# custom field ID from configuration
orgCodeCFID = data['custom_field_id']['org_code_cf_id']
activityCodeCFID = data['custom_field_id']['activity_code_cf_id']
# get all the unique date from a column specifically, this will act as a checker for the menu
orgCodeList = [x for x in list(set(importFile['org_code'].tolist())) if str(x) != "nan"]
activityCodeList = [x for x in list(set(importFile['activity_code'].tolist())) if str(x) != "nan"]
# separate custom field as existing data
orgCodeDataExisting = [item for item in existingFieldForTheList if item['id'] == orgCodeCFID][0]
activityCodeDataExisting = [item for item in existingFieldForTheList if item['id'] == activityCodeCFID][0]

# function that validates a custom field menu based on the excel column that it is mapped into; we creating a new list ^ that is unique so we can identify it
def validateCustomFieldsMenufromExcelColumn(cfFieldMenu):
    # we only making it true if we are clear on every custom field menu
    isValid = True

    for cfField in cfFieldMenu:
        availableOptions = cfField['field']['existing_from_cf']['type_config']['options']
        existingOptionsonExcel = cfField['field']['options']
        for existingOption in existingOptionsonExcel:
            validateExistingOption = [item for item in availableOptions if item['name'] == str(existingOption)]
            if not validateExistingOption:
                print("[" + str(existingOption) + "] is not existing on the menu option for custom field id:" + str(cfField['field']['existing_from_cf']['id']) + " - " + str(cfField['field']['existing_from_cf']['name']))
                isValid = False

    print("######### VALIDATION COMPLETE #########")
    return isValid



######### IMPORTING START HERE #########

# validate first before move to importing, this goes only one time so you can clear this out probably to reduce the process (but not a big deal)
if (validateCustomFieldsMenufromExcelColumn([
    {'field':{'field_id':orgCodeCFID,'options':orgCodeList,'existing_from_cf':orgCodeDataExisting}},
    {'field':{'field_id':activityCodeCFID,'options':activityCodeList,'existing_from_cf':activityCodeDataExisting}}])):

    # create a new list without any header
    forDescriptionListIndex = importFile.columns.tolist()
    # items to be removed for DESCRIPTION ONLY
    forDescriptionListIndex = [ele for ele in forDescriptionListIndex if ele not in {"job_#", "job_name","due_date","org_code","activity_code"}]

    # for test run only comment if not using
    # limit = 10

    # now loop
    for index in range(0,len(importFile)):

        # format due date
        dueDate = int(importFile["due_date"][index].timestamp() * 1000)
        # format description
        description = ""
        for descriptionCol in forDescriptionListIndex:
            description += str("**" + descriptionCol.replace("_", " ").title()) + ":** " 
            description += "N/A \n\n" if pd.isna(importFile[descriptionCol][index]) else str(importFile[descriptionCol][index]).strip() + "\n\n"
        
        # find the index via string value
        orgCodeIndex = "" if pd.isna(importFile["org_code"][index]) else [item for item in orgCodeDataExisting['type_config']['options'] if item['name'] == str(importFile["org_code"][index])][0]['orderindex']
        activityCodeIndex = "" if pd.isna(importFile["activity_code"][index]) else [item for item in activityCodeDataExisting['type_config']['options'] if item['name'] == str(importFile["activity_code"][index])][0]['orderindex']
        
        # set the payload
        taskCreatePayload = {
            "name": "#" + str(importFile["job_#"][index]) + " - " + str(importFile["job_name"][index]),
            "markdown_description": str(description),
            "due_date": dueDate,
            "custom_fields": [
                {"id": str(orgCodeCFID), "value":  orgCodeIndex},
                {"id":  str(activityCodeCFID), "value": activityCodeIndex},
            ],
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

        #for test run only comment if not using
        # if index >= limit : break

print("[IMPORT COMPLETE]: Imported for a total of " + str(len(importFile)))