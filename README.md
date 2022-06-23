# Streaming Platform Analysis
## Goal
For each of the popular film streaming platform(Netflix, Disney+, Amazon Prime...etc):
1. Listing out the most popular films in each region(country) for the past two years
2. Listing out the films that are **only** popular in that region(country) for the past two years (i.e. ruling out the world-trending movies)
3. Make an analysis on the movie production power(i.e. how many top 10 films are produced in the country) of each country
4. Count the common keywords of all top 10 films to find out the **key elements** that make the films well-accepted for general people
## Methods
### 1. Get the Data 
a. Scrape the FlixPatrol website to acquire the **top 10** Films on **every platform** in **every country** for a specific period

b. Scrape the FlixPatrol website to acquire the **detail information** of each film that is on the top 10 baord

c. Utilize the TMDb website API to acquire the **detail information** of each film that is on the Flixpatrol top 10 board
### 2. Data Cleaning and Prepocessing
a. Fill in the null value and correct typos or originating errors

b. Make word extraction from "summary" of each film to select reasonable "keywords" (especially those films lacking in keywords tags from TMDb website)

c. Make a cluster analysis on each country/region to see who has the similar preference over films

d. Export the data as csv files
### 3. Visulization
Use csv files to build Tableau dashoard

## Links
1. [FlixPatrol](https://flixpatrol.com/)
2. [TMDb](https://www.themoviedb.org/)
