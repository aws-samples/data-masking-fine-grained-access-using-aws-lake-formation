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

import pandas as pd
from faker import Faker

# create some fake data
fake = Faker(locale='en_US')

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
df_fake_credit_cards.to_csv('fake_credit_cards.csv')
df_fake_banks.to_csv('fake_banks.csv')