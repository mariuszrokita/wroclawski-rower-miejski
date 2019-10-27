# Wroclaw Nextbike - analysis of the use of bicycles operated by Wrocławski Rower Miejski

The [Nextbike Polska](https://nextbike.pl/en/) is a city bike sharing system available in several dozen of cities and towns throughout Poland. One of those cities is Wrocław.

The aim of this project is to analyze bike sharing data and draw insights that could be further shared with local authorities. The data is available at the [Wroclaw Open Data](https://www.wroclaw.pl/open-data/dataset/wrmprzejazdy_data) website.

## Getting started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

You need to have an active Azure subscription. If you don't have it, create a [free account](https://azure.microsoft.com/en-us/free/) before you begin.

### Virtual environment

First of all, create a brand new virtual environment to have an isolated workspace with specific package installs. It is extremely useful when developing multiple projects that require different dependencies.

```bash
conda create --name nextbike python=3.6.8
conda activate nextbike
```

### Install dependencies

```bash
pip3 install -r requirements.txt
```

It is also worth installing the Jupyter Notebook Extensions:

```bash
pip3 install jupyter_contrib_nbextensions && jupyter contrib nbextension install
```

## Project Structure

The project consists of 3 components:

* Data importing
* Bike rentals
* Bike availability

### Data Importing

The main purpose of this component is to periodically import data (bike rentals, bike availability) and to persists those data in a separate storage.

The Data Importer compontent has been implemented as two time-triggered Azure Functions.

#### Deployment - Local environment

In order to deploy function app locally run the following commands:

```bash
cd src/azurefunctions
func host start
```

#### Deployment to Azure Cloud

Do the following:

1. Install [jq](https://stedolan.github.io/jq/) library.

1. Set proper configuration in the `./deployment/config.sh` file.

1. Run the following commands:

```bash
cd data-importing/deployment
sh 01-provision-azure-resources.sh
sh 02-deploy-azure-functions.sh
```
