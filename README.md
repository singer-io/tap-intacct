# tap-intacct
Tap for [Intacct](https://www.sageintacct.com/). This tap does not interact with the Intacct API, it relies on Intaccts Data Delivery Service to publish
CSV data to S3 which this tap will consume.

## Quick start

### Install

We recommend using a virtualenv:

```bash
> virtualenv -p python3 venv
> source venv/bin/activate
> pip install tap-intacct
```

### Create the config file

The Intacct Tap requires a start_date, a bucket, and an Intacct company ID to function.

  **start_date** - an initial date for the Tap to extract data
  **bucket** - The name of an S3 bucket where the Intacct DDS is outputing data
  **company_id** - The Company ID used to login to the Intacct UI
  **path** (optional) - An optional path configured in the Intacct UI for use in the S3 bucket

Below are the additional properties, to add in config if running this tap using proxy AWS account as middleware:
```
    "proxy_account_id": "221133445566",
    "proxy_role_name": "proxy_role_with_bucket_access"
```
Proxy AWS account will act as a middleware.
- **proxy_account_id**: This is the Proxy AWS account id.
- **proxy_role_name**: This is the Proxy IAM role that allows the product AWS account to assume it and then use this role to access S3 bucket in your account.

### Configure your S3 Bucket

This tap uses the [boto3](https://boto3.readthedocs.io/en/latest/index.html) library for accessing S3. The [credentials](https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration)
used by boto will also need access to the S3 bucket configured in the above config.

### Run the Tap in Discovery Mode

`tap-intacct -c config.json --discover`


---

Copyright &copy; 2018 Stitch
