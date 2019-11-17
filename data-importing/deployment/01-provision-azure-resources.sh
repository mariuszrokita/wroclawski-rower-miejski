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

echo "*** STEP 3: Create Application Insights... ***"
az resource create \
    --resource-group $RESOURCE_GROUP \
    --resource-type "Microsoft.Insights/components" \
    --name $APPLICATION_INSIGHTS_NAME \
    --location $REGION \
    --properties '{"Application_Type":"web"}'

echo "*** STEP 4: Create storage account... ***"
az storage account create \
    --resource-group $RESOURCE_GROUP \
    --location $REGION \
    --name $STORAGE_NAME \
    --sku Standard_LRS \
    --kind StorageV2 \
    --access-tier Cool

echo "*** STEP 5: Create storage containers... ***"
az storage container create \
    --name $BIKE_RENTALS_CONTAINER_NAME \
    --account-name $STORAGE_NAME \
    --public-access off \
    --subscription $SUBSCRIPTION_ID

az storage container create \
    --name $BIKE_AVAILABILITY_CONTAINER_NAME \
    --account-name $STORAGE_NAME \
    --public-access off \
    --subscription $SUBSCRIPTION_ID

echo "*** STEP 5: Creating function app... ***"
az functionapp create \
    --name $FUNCTION_APP_NAME \
    --subscription $SUBSCRIPTION_ID \
    --resource-group $RESOURCE_GROUP \
    --storage-account  $STORAGE_NAME \
    --consumption-plan-location $CONSUMPTION_PLAN_LOCATION \
    --app-insights $APPLICATION_INSIGHTS_NAME \
    --runtime python \
    --os-type Linux
