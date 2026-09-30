create database sql_eventstore;
use sql_eventstore;
CREATE TABLE Event(eventType VARCHAR(255), body BLOB, version INT PRIMARY KEY, effective_timestamp datetime);
