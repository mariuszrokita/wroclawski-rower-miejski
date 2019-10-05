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

echo "*** STEP 3: Create Application Insights...***"
az resource create \
    --resource-group $RESOURCE_GROUP \
    --resource-type "Microsoft.Insights/components" \
    --name $APPLICATION_INSIGHTS_NAME \
    --location $REGION \
    --properties '{"Application_Type":"web"}'

echo "*** STEP 4: Creating function app...***"
az functionapp create \
    --name $FUNCTION_APP_NAME \
    --subscription $SUBSCRIPTION_ID \
    --resource-group $RESOURCE_GROUP \
    --storage-account  $STORAGE_NAME \
    --consumption-plan-location $CONSUMPTION_PLAN_LOCATION \
    --app-insights $APPLICATION_INSIGHTS_NAME \
    --runtime python \
    --os-type Linux
