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
historic_records_url=`jq -r '.Values.historic_records_url' local.settings.json`
storage_account_name=`jq -r '.Values.storage_account_name' local.settings.json`
storage_account_key=`jq -r '.Values.storage_account_key' local.settings.json`
bike_rentals_container_name=`jq -r '.Values.bike_rentals_container_name' local.settings.json`
bike_availability_data_url=`jq -r '.Values.bike_availability_data_url' local.settings.json`
bike_availability_container_name=`jq -r '.Values.bike_availability_container_name' local.settings.json`

az functionapp config appsettings set \
	--name $FUNCTION_APP_NAME \
	--resource-group $RESOURCE_GROUP \
	--settings \
		historic_records_url=$historic_records_url \
		storage_account_name=$storage_account_name \
		storage_account_key=$storage_account_key \
		bike_rentals_container_name=$bike_rentals_container_name \
		bike_availability_data_url=$bike_availability_data_url \
		bike_availability_container_name=$bike_availability_container_name
