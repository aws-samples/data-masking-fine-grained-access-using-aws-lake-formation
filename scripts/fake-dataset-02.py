# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

import boto3
import pandas as pd
from faker import Faker
import logging

# create some fake data
fake = Faker(locale='en_US')

# Obter a AWS Region
session = boto3.Session()
region = session.region_name

# Obter a AWS Account ID
sts = boto3.client('sts')
account_id = sts.get_caller_identity()["Account"]

# Criar um cliente S3
s3 = boto3.client('s3')


BUCKET_NAME= f'assets-workshop-reinforce-us-east-1-{account_id}'
S3_FILE_PATH='csv/fake_profile.csv'
LOCAL_FILE_PATH='fake_profile.csv'

print('BUCKET_NAME', BUCKET_NAME)

def upload_file(folder_name,file_name_csv, file_name_parquet):
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_name_csv, BUCKET_NAME, f"csv/{folder_name}/{file_name_csv}")
        print(f"Arquivo CSV '{file_name_csv}' enviado com sucesso para o S3 em csv/{file_name_csv}.")
        s3_client.upload_file(file_name_parquet, BUCKET_NAME, f"parquet/{folder_name}/{file_name_parquet}")
        print(f"Arquivo Parquet '{file_name_parquet}' enviado com sucesso para o S3 em csv/{file_name_parquet}.")
    except:
        print(f"Arquivo CSV '{file_name_csv}' enviado com ERRO para o S3 em csv/{file_name_csv}.")
        return False
    return True


fake_profile = [fake.simple_profile() for x in range(100)]

fake_credit_cards = [
    {'cred_card_provider': fake.credit_card_provider(),
     'card_number': fake.credit_card_number(),
     'card_expiration': fake.credit_card_expire(),
     'card_security_code': fake.credit_card_security_code()}
    for x in range(100)]

fake_banks = [
    {'account_number': "XYZ-" + str(x),
    'iban': fake.iban(),
    'bban': fake.bban()}
    for x in range(100)]

#generate data frame     

df_fake_profile = pd.DataFrame(fake_profile)
del df_fake_profile['address']
df_fake_profile.columns = df_fake_profile.columns.str.replace('sex', 'gender')

df_fake_credit_cards = pd.DataFrame(fake_credit_cards)

df_fake_banks = pd.DataFrame(fake_banks)

# data frame to csv

df_fake_profile.to_csv('fake_profile.csv')
df_fake_profile.to_parquet('fake_profile.parquet')
upload_file('profile','fake_profile.csv', 'fake_profile.parquet')

df_fake_credit_cards.to_csv('fake_credit_cards.csv')
df_fake_credit_cards.to_parquet('fake_credit_cards.parquet')
upload_file('credit_cards','fake_credit_cards.csv', 'fake_credit_cards.parquet')

df_fake_banks.to_csv('fake_banks.csv')
df_fake_banks.to_parquet('fake_banks.parquet')
upload_file('bank','fake_banks.csv', 'fake_banks.parquet')

