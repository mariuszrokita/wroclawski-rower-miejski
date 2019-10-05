#!/bin/sh

# load configuration
. config.sh

echo "*** STEP 1: Checking Azure CLI version... ***"
az --version
 
echo "*** STEP 2: Sign in to Azure Portal... ***"
az account show 1> /dev/null
if [ $? != 0 ];
then
	az login
fi

echo "*** STEP 3: Publishing function app... ***"
cd ../src/azurefunctions
func azure functionapp publish $FUNCTION_APP_NAME --build remote

echo "*** STEP 4: Set environment variables... ***"

historic_records_url=`jq '.Values.historic_records_url' local.settings.json`
storage_account_name=`jq '.Values.storage_account_name' local.settings.json`
storage_account_key=`jq '.Values.storage_account_key' local.settings.json`
storage_container_name=`jq '.Values.storage_container_name' local.settings.json`

# echo $historic_records_url
# echo $storage_account_name
# echo $storage_account_key
# echo $storage_container_name

az functionapp config appsettings set \
	--name $FUNCTION_APP_NAME \
	--resource-group $RESOURCE_GROUP \
	--settings \
		historic_records_url=$historic_records_url \
		storage_account_name=$storage_account_name \
		storage_account_key=$storage_account_key \
		storage_container_name=$storage_container_name
