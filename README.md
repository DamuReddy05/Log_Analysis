# Project1 in Udacity Fullstack Web Developer NanoDegree program

## LOG ANALYSIS Project:

## Description About The Project:
 --> Three tables by names articles, authors, log will be given to the student in newsdata.sql. An Udacity student has to write three queris based on the requirments of Udacity. This should be done using postgresql.

## Queries to write are for:

1. What are the three most popular articles of all time?

2. Who are the most popular authors of all time?

3. The day with more than 1% of the requests that led to an error?


## Pre requisites for this projects:
 --> Python3
 --> Vagrant
 --> Virtual Box

## Setting up the envirment:
 -->Download and install virtual box from [here](https://www.virtualbox.org).
 -->Download vagrant from (https://www.vagrantup.com/downloads.html).
 -->Do (`vagrant up`) after opening Git Bash.
 -->Do (`vagrant ssh`) after doing vagrant up.
 -->Download database from [here](https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip).                        
 -->Extract the zip file under vagrant folder by name log_analysis

## Running the project:
 -->Go to Log_Analysis folder by using the command (`cd /vagrant/Log_Analysis`)
 -->Run the project file by using the command (`python loganalysis.py`)
 -->Then the output would be as shown below.


# OUTPUT:

### 1. What are the three most popular articles of all time?
    1 . Candidate is jerk, alleges rival --> 338647 views
    2 . Bears love berries, alleges bear --> 253801 views
    3 . Bad things gone, say good people --> 170098 views

### 2. Who are the most popular authors of all time?
    1 . Ursula La Multa --> 507594 views
    2 . Rudolf von Treppenwitz --> 423457 views
    3 . Anonymous Contributor --> 170098 views
    4 . Markoff Chaney --> 84557 views

### 3. The day with more than 1% of the requests that led to an error?
    1 . 2016-07-17 --> 2.263 %

