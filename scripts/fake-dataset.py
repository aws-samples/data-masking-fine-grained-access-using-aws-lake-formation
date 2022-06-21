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