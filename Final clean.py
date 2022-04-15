import requests
import pandas as pd
import csv
import time
import twint
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

#Globals
print("Welcome to twitter scraper.")
searchTerm = input("Enter a search term: ")
searchLimit = int(input("Enter how many tweets to search for: "))
followerLimit = int(input("Enter how many followers they should have: "))
def getKeys(mode="main"):
    if(mode=="sandbox"):
        api_key = ''
        api_secret = ''
        bearer = ''
        access_token = ''
        access_secret = ''
        sandbox_key = True
    elif(mode=="main"):
        api_key = ''
        api_secret = ''
        bearer = ''
        access_token = ''
        access_secret = ''
        sandbox_key = False
    else:
        return -1
    return api_key, api_secret, bearer, access_token, access_secret, sandbox_key

def searchTweets(searchTerm,searchLimit):
    c = twint.Config()
    c.Search = str(searchTerm) # search for all tweets containing search term
    c.Limit = int(searchLimit)      # number of tweets to scrape
    c.Pandas = True # Enable pandas integration
    c.Hide_output = True # Hide search output from terminal
    c.Filter_retweets = True # Set to true to exclude retweets
    # Enable if storing as csv
    # c.Store_csv = True       # store tweets in a csv file
    # fileName = f'{searchTerm}_tweets.csv'
    # c.Output = fileName     # path to csv file
    twint.run.Search(c)

    Tweets_df = twint.storage.panda.Tweets_df
    return Tweets_df

def parseLink(link):
    if("t.co" in link):
        # print("Twitter link found")
        reqUrl = requests.get(link, verify=False, timeout=10).url
        return reqUrl
    else:
        return link

if(getKeys("main")==-1):
    raise Exception('Invalid mode.')
    configPackage += "\nInvalid mode"
else:
    consumer_key, consumer_secret, bearer, access_token, access_token_secret, sandbox_key = getKeys("main")

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_url(usernames):

    search_url = "https://api.twitter.com/2/users/by" # Collecting users

    #change params based on the endpoint you are using
    query_params = {'usernames': usernames, # A comma separated list of screen names, up to 100 are allowed in a single request.
                    'user.fields': 'description,id,name,public_metrics,url',
                    }
    return (search_url, query_params)

def connect_to_endpoint(url, headers, params, next_token = None):
    # params['next_token'] = next_token   #params object received from create_url function
    retries = 0
    while(retries<5):
        response = requests.request("GET", url, headers = headers, params = params, timeout=10)
        # print("Endpoint Response Code: " + str(response.status_code))
        if response.status_code == 200:
            retries = 0
            return response.json()
        elif response.status_code == 429:
            # raise Exception(response.status_code, response.text)
            retries = retries + 1
            time.sleep(300)

def readList(tweetsdf):
    userList = tweetsdf['username'].tolist()
    userList = list(dict.fromkeys(userList)) # Convert into dict then list to remove duplicates
    return(userList)

def update_progress(progress):
    print("\rProgress: [{0:50s}] {1:.1f}%".format('#' * int(progress * 50), progress*100), end="", flush=True)

def parseLink(link):
    if("t.co" in link):
        reqUrl = requests.get(link, verify=False, timeout=10).url
        return reqUrl
    else:
        return link

print(f"Searching for term \"{searchTerm}\" with approximate limit of {searchLimit} tweets.")
print("This may take some time. Please be patient.")
dfTweets = searchTweets(searchTerm, searchLimit) # Run searchTweets and store in dataframe dfTweets
print(f"Found {len(dfTweets.index)} tweets.")

print("Preparing to scrape users")
headers = create_headers(bearer)
print("Generating user list")
usersList = readList(dfTweets)
print(f"Found {len(usersList)} unique users.")
start_time = time.time()
update_progress(0)
for i in range(0, len(usersList), 100):
    chunk = usersList[i:i + 100]
    usersCsv = ",".join(map(str, chunk))
    # print(usersCsv)
    url = create_url(usersCsv)
    json_response = connect_to_endpoint(url[0], headers, url[1])
    usersdf = pd.json_normalize(json_response['data'])
    progress = i/len(usersList)
    update_progress(progress)
    if i==0:
        # usersdf.to_csv('searchUsers.csv', index=False)
        finaldf = usersdf
    else:
        # print("updating")
        # usersdf.to_csv('searchUsers.csv', mode='a', header=False, index=False)
        finaldf = finaldf.append(usersdf)
update_progress(1)
print("")
timestr = time.strftime("%Y%m%d-%H%M%S")+"-users.csv"
print(f"Saving to {timestr}")
finaldf = finaldf[finaldf['public_metrics.followers_count'] >= followerLimit]
finaldf = finaldf[['username','public_metrics.followers_count','url']]
finaldf.to_csv(timestr, index=False)

end = (time.time() - start_time)
print(f"Finished searching in {end} seconds.")

print("Cleaning data.")
start_time = time.time()
file = open(timestr)
countReader = csv.reader(file)
totalLines= len(list(countReader))
cleaned = timestr.replace(".csv","-clean.csv")
with open(timestr) as inf, open(cleaned, 'w',newline='') as outf:
    reader = csv.reader(inf)
    writer = csv.writer(outf)
    lineCount = 0
    for line in reader:
        if lineCount == 0:
            line.append("handle")
            writer.writerow(line)
            lineCount += 1
        else:
            try:
                line[2] = parseLink(line[2])
            except:
                pass
            line.append("https://twitter.com/"+line[0])
            writer.writerow(line)
            lineCount += 1
        update_progress(lineCount/totalLines)
print("")
end = (time.time() - start_time)
print(f"Finished cleaning data in {end} seconds.")
print(f"Saved clean data to {cleaned}")
input("Scraping done. Press enter to exit.")
