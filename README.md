# tweet-processing
ETL pipeline that extracts tweets from the accounts a user is following using the twitter api v2. The tweets are then transformed into a format to be used for natural langauge processing (NLP), and loaded into a Postgres database.

## **ETL Pipeline High Level Design**

![alt_text](https://github.com/oiannace/tweet-processing/blob/master/aws-architecture-diagram.png)  

## **Requirements**
The following are requirements for using this code:  
- AWS account & IAM user created: [AWS Homepage](https://aws.amazon.com/?nc2=h_lg)  
- AWS CLI [installed](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and [configured](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html), and [access key for IAM user created](https://docs.aws.amazon.com/cli/latest/userguide/cli-services-iam-create-creds.html)
- Twitter account & API Key, Secret, and Bearer Token created: [Twitter Developer Docs](https://developer.twitter.com/en/docs/apps/overview)

## **Setting up the infrastructure in Amazon Web Services (AWS)**

Run the insert_account_id script with your AWS account id asan argument as follows:
```
$ ./insert_account_id ###########
```
This inserts your account id into the setup script, and the json files detailing the role id. The following script will create the lambda functions and their associated roles, the s3 bucket, and the PostgreSQL RDS database instance.
```
$ ./setup_script
```

## **Python Dependencies**

The dependencies are added to lambda functions in the form of layers. A layer is created with a provided zip file with relevant libraries installed. More information can be found here: https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html  
  
In order to create layers that are compatible with the lambda operating system, an ec2 instance was created in order to install and package the dependencies. This is accomplished by executing the following script:
```
./ec2-instance-setup
```


#### lambda-extraction-function
Python libraries required: requests, boto3

#### lambda-transformation-function
Python libraries required: boto3, pandas, nltk

#### lambda-loading-function
Python libraries required: pandas, psycopg2, sqlalchemy, boto3
