CREATE DATABASE buyonline;

USE buyonline;

CREATE TABLE product(
id INT PRIMARY KEY,
name VARCHAR(128),
category VARCHAR(128),
price FLOAT,
last_updated TIMESTAMP)
