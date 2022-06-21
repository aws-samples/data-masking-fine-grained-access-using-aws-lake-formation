/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
 
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


