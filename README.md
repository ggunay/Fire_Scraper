Great Scraper with Firebase which:

1-) Scrapes list of URLs from the main page (base_url)

2-) Scrapes each URL, parses data inside and checks if it has subpages

3-) If it does, Scrapes and parses all of the subpages as well.

4-) Writes parsed data to Firebase

speed-optimized thanks to grequest, threads, queues and batch connections to DB with safety-checks.

In more detail:

Makes use of cookies from file and random user-agents for connections. Detects if a page is scraped correctly or not by checking for a specific tag in the html (this tag should be common across all the pages). If this tag is not there, every URL is re-scraped until max number is reached. Max number is tracked per URL. 

Once a page is properly scraped:

-Checks for the existance of subpages by tag and attriute and if it exists, gets the total number of subpages and puts each subpage into the queue of URLs to be scraped
-divides its html into blocks (e.g. posts) by tag and attribute

Parses each block and gets the values

writes those values in bathes to a Firebase document
