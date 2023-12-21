#!/bin/bash

# Check if aws cli is installed
if ! command -v aws &> /dev/null
then
    echo "AWS CLI could not be found, please install it first."
    exit
fi

read -p "Enter Profile Name: " profile_name
read -p "Enter Access Key: " access_key
read -p "Enter Secret Access Key: " secret_access_key
read -p "Enter Default Region: " default_region
read -p "Enter Output Format: " output_format

# Create the new profile
aws configure set aws_access_key_id $access_key --profile $profile_name
aws configure set aws_secret_access_key $secret_access_key --profile $profile_name
aws configure set region $default_region --profile $profile_name
aws configure set output $output_format --profile $profile_name

echo "AWS CLI profile: $profile_name has been created successfully!"

echo "view command: nano ~/.aws/credentials"
echo "view command: nano ~/.aws/config"
