# wroclawski-rower-miejski

## Set up virtual environment

```bash
conda create --name nextbike python=3.6.8

conda activate nextbike
```

## Install dependencies

```bash
pip3 install -r requirements.txt
```

It is also worth installing the Jupyter Notebook Extensions:

```bash
pip3 install jupyter_contrib_nbextensions && jupyter contrib nbextension install
```

## Deployment

### Local environment

In order to deploy function app locally run the following commands:

```bash
cd src/azurefunctions
func host start
```

### Deployment to Azure Cloud

Do following:

1. Install [jq](https://stedolan.github.io/jq/) library.

1. Set proper configuration in the `./deployment/config.sh` file.

1. Run the following commands:

```bash
cd deployment
sh 01-provision-azure-resources.sh
sh 02-deploy-azure-functions.sh
```
