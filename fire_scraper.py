import sys
import random
import grequests
import http.cookiejar
import datetime
import firebase_admin
import queue
from threading import Thread
import time
from datetime import datetime
from bs4 import BeautifulSoup, SoupStrainer, Tag
from firebase_admin import credentials
from firebase_admin import firestore

# Read the service account key file
creds = credentials.Certificate('FIle for Firebase credentials.json')
# Initialize the Firebase app
firebase_admin.initialize_app(creds)
# Get a reference to the Firestore database
db = firestore.client()



# Read the cookies from a file
cookie_jar = http.cookiejar.MozillaCookieJar()
cookie_jar.load('cookies.txt')

# Convert the cookies to a dictionary
cookies = {}
for cookie in cookie_jar:
    cookies[cookie.name] = cookie.value


# Configurable variables
url_base = "https://test.com"  # base URL to start from
parent_tag_to_find_links = "ul"  # tag to find links inside
tag_to_find_links = "li"  # tag to find links inside
attr_to_find_links = "class"  # attribute to find links inside the tag
attr_to_find_links_value = "topic-list"  # attribute to find links inside the tag
tag_to_check_existence = "li"  # tag to check existence of
attr_to_check_existence = "id"  # attribute to check existence of the tag
attr_value_to_check_existence = "post-item"  # value of the attribute to check existence of the tag
tag_to_find_subpages = "div"  # tag to find the number of subpages
attr_to_find_subpages = "class"  # attribute to find the number of subpages
attr_to_find_subpages_value = "pager"  # attribute to find the number of subpages
lastpage_value_of_subpages = "pagecount" # in which attribute the actual last page value is
divide_html_tag = "ul"  # tag to divide HTML into blocks
divide_html_attr = "id"  # attribute to divide HTML into blocks
divide_html_attr_value = "post-item-list"  # value of attribute to divide HTML into blocks
divide_html_block_tag = "li"  # value of attribute to divide HTML into blocks
sub_page_parameter = "&p="
threads = []

MAX_RETRIES = 3  # maximum number of retries per link
DELAY = 4  # delay between requests in seconds

subtag_1 = "data-id"  # name of subtag 1
subtag_2 = "data-modified"  # name of subtag 2
subtag_3 = "data-datetime"  # name of subtag 3
subtag_4 = "data-integer"  # name of subtag 4
subtag_5 = "data-text-1"  # name of subtag 5
subtag_6 = "data-text-2"  # name of subtag 6
subtag_7 = "data-text-3"  # name of subtag 7

firebase_document_name=u'posts'

link_queue = queue.Queue()
q_database = queue.Queue()

# Set the batch size for database connections
BATCH_SIZE = 10


headers = [
    {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.3029.110 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'},
    {'User-Agent': 'Mozilla/5.0 (iPad; CPU OS 14_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'},
]


def retrieve_html(url):

  response = grequests.get(url, headers=random.choice(headers),cookies=cookies)
  # Wait for the response to complete
  response = grequests.map([response])[0]
  # Return the response content
  return response.content


def find_links(html):
  soup = BeautifulSoup(html, "html.parser")
  links = []
  for li in soup.find(parent_tag_to_find_links, attrs={attr_to_find_links: attr_to_find_links_value}).find_all(tag_to_find_links):
    a_tag = li.find("a")
    if a_tag:
      links.append(a_tag["href"])
  return links




def check_existence(html):
  # Parse the HTML and check if the specified tag with the specified attribute and value exists
  soup = BeautifulSoup(html.content, "html.parser")
  #tag = soup.find(tag_to_check_existence, attrs={attr_to_check_existence: attr_value_to_check_existence})
  tag = soup.find(tag_to_check_existence, id=attr_value_to_check_existence)
  if tag is None:
    print("Check Existance tag for: "+str(html.url)+ " is "+ str(tag))
  return tag is not None


def find_subpages(html):
  # Parse the HTML and find the number of sub pages inside the specified tag
  soup = BeautifulSoup(html.content, "html.parser")
  tag = soup.find(tag_to_find_subpages, attrs={attr_to_find_subpages: attr_to_find_subpages_value})
  if tag is not None:
    #return int(tag.get("data-pagecount"))
    return int(tag.get(lastpage_value_of_subpages))
  return 0

def divide_html(html):
  #print("dividing html into blocks")
  soup = BeautifulSoup(html.content, "html.parser")
  blocks = soup.find("ul", id=divide_html_attr_value).find_all(divide_html_block_tag)
  #print("blocks returned: " + str(blocks))
  #sys.exit(0)
  # if blocks is None:
  #   print("blocks none")
  return blocks


def process_block(block):
  # Extract the data from the specified block of HTML
  data = {}
  data[subtag_1] = block.get(subtag_1)
  data[subtag_2] = block.get(subtag_2)
  data[subtag_3] = block.get(subtag_3)
  data[subtag_4] = block.get(subtag_4)
  data[subtag_5] = block.get(subtag_5)
  data[subtag_6] = block.get(subtag_6)
  data[subtag_7] = block.get(subtag_7)
  return data

  # Write the data to the Firestore database in batches
def write_to_database():
  batch = db.batch()
  count = 0

  # Consume items from the queue and process them
  while True:
  #while not q_database.empty():
    try:
      # Get an item from the queue, returning immediately if the queue is empty
      data_list = q_database.get(block=False)
      print("writing to database: " + str(data_list))

      # Iterate over the list of dictionaries
      for data in data_list:
        # Get a reference to the document
        doc_ref = db.collection(firebase_document_name).document(data[subtag_1])
        # Add the dictionary to the batch
        batch.set(doc_ref, data)
        count += 1
        # If the batch size has been reached, commit the batch
        if count % BATCH_SIZE == 0:
          batch.commit()
          batch = db.batch()

      # Commit any remaining items in the batch
      batch.commit()

      #print("Consumed {} items".format(len(data_list)))
    except queue.Empty:
      break


def process_page(html):
  # Process a page by dividing it into blocks and extracting the data from the blocks
  #print("processing " + page_url)
  if html is None:
    print("HTML none for " + str(html.url))
  else:

    blocks = divide_html(html)
    data = []
    for block in blocks:
      data.append(process_block(block))
    #print("data " + str(data))
    q_database.put(data)


def process_link(url_base,link_queue):
  # Process a link by retrieving the HTML, dividing it into blocks, and extracting the data from the blocks

  # Get the links from the queue
    links = []
    while not link_queue.empty():
      links.append(link_queue.get())

    link_htmls = grequests.map((grequests.get(u["url"],headers=random.choice(headers),cookies=cookies) for u in links), size=5)

    for i, link_html in enumerate(link_htmls):

      if link_html is not None and check_existence(link_html):
        #link = next(link for link in links if url_base + str(link["url"]) == str(link_html.url))
        url = links[i]["url"]
        type = links[i]["type"]
        retries = links[i]["retries"]

        # If the link is a URL, process the data from the HTML
        if type == "url":
          pages = find_subpages(link_html)
          #data = process_page(link_html)
          if pages:
            for page in range(2, pages + 1):
              link_queue.put({"url": link_html.url+ sub_page_parameter + str(page), "type": "sub-url", "retries": 0})
            print("added ")
          print("inside type url " + str(link_html.url))
          thread = Thread(target=process_page, args=(link_html,))
          # Start the thread
          thread.start()
          threads.append(thread)
          #print("inside type url after thread")

        # If the link is a sub-URL
        else:
            print("inside type sub-url before thread " + str(link_html.url))
            thread = Thread(target=process_page, args=(link_html,))
            # Start the thread
            thread.start()
            threads.append(thread)
            #print("inside type url after thread")

      else:

        # If the number of retries has not reached the maximum, add the link back to the queue
        if links[i]['retries'] < MAX_RETRIES:
          print(f"adding retry to queue for: {links[i]['url']}")

          link_queue.put({"url": links[i]['url'], "type": links[i]['type'], "retries": links[i]['retries']+1})
        # If the number of retries has reached the maximum, process the data from the HTML
        else:
          # TODO sth with max retry data = process_page(link_html)
          print(f"{links[i]['url']}  not retrieved after all retries")

    return True

def parser(first_links):

  for link in first_links:
    link_queue.put({"url": url_base+link, "type": "url", "retries": 0})

  # Process the links in the queue
  while not link_queue.empty():
    try:
      process_link(url_base,link_queue)
    except Exception as e:
      print(e) #TODO sth with error
  for thread in threads:
    thread.join()


def main():
  retries = 0
  while True:
    try:
      # Retrieve the HTML of the starting webpage
      html = retrieve_html(url_base)

      # Find the links inside the HTML and add them to the queue
      first_links = find_links(html)
      break  # exit the loop when there is no exception
    except AttributeError:
      retries += 1
      if retries >= MAX_RETRIES:  # exit the loop when the maximum number of retries is reached
        print("Failed to retrieve the base url or tags for finding links don't exist")
        break
      time.sleep(DELAY)  # wait for a delay before retrying

  producer_thread = Thread(target=parser,args=[first_links],)
  #producer_thread = Thread(target=process_link, args=(url_base, link_queue))
  producer_thread.start()



  try:
    producer_thread.join()
#    write_to_database_thread.join()
  except Exception as e:
    # Handle any exceptions that may be raised by the thread
    print("Exception raised in thread:", e)
  except KeyboardInterrupt:
    # Handle the keyboard interrupt event
    print("Keyboard interrupt raised, exiting")
    sys.exit(1)

    # consumer
  write_to_database_thread = Thread(target=write_to_database)
  write_to_database_thread.start()
  try:
    write_to_database_thread.join()
  except Exception as e:
    # Handle any exceptions that may be raised by the thread
    print("Exception raised in thread:", e)
  except KeyboardInterrupt:
    # Handle the keyboard interrupt event
    print("Keyboard interrupt raised, exiting")
    sys.exit(1)

if __name__ == "__main__":
  main()
