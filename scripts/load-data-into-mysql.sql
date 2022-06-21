CREATE DATABASE IF NOT EXISTS dataset;
USE dataset;

CREATE TABLE IF NOT EXISTS dataset.card(
    id INT NOT NULL, 
    cred_card_provider varchar(200) not null, 
    card_number varchar(200) not null, 
    card_expiration varchar(200) not null, 
    card_security_code varchar(200) not null 
);
    
    
CREATE TABLE IF NOT EXISTS dataset.bank( 
    id INT NOT NULL,
    account_number varchar(200) not null, 
    iban varchar(200) not null, 
    bban varchar(200) not null 
);


CREATE TABLE IF NOT EXISTS dataset.customer( 
    id INT NOT NULL,
    username varchar(200) not null, 
    name varchar(200) not null, 
    gender  varchar(200) not null, 
    mail  varchar(200) not null, 
    birthdate  varchar(200) not null 
);



LOAD DATA LOCAL INFILE '/tmp/fake_profile.csv' INTO TABLE customer FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/tmp/fake_credit_cards.csv' INTO TABLE card FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/tmp/fake_banks.csv' INTO TABLE bank FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;


