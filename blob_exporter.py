import logging
import asyncio
import aiohttp
import base64
import pandas as pd
import time

# Configure logging to print to stdout
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#  New Relic user API key NRAK-***
NEW_RELIC_API_KEY = ''
#  New Relic account ID
NEW_ACCOUNT_ID = ''
# EXAMPLE -> "SELECT message, blob(`newrelic.ext.message`) FROM Log WHERE lorem = 'true' since 1 hour ago"
QUERY = ""
LIMIT = 20  # Limit for each query the blob query max is 20 records
# https://docs.newrelic.com/docs/logs/ui-data/long-logs-blobs/
# attribute containing the first 4,094 characters
log_attribute='message'
# attribute to decode, this is the next 128,000 UTF-8 bytes that were stored as blob
blob_attribute='newrelic.ext.message'
blob_data = []
num_concurrent_requests = 10
panda_frames_lst=[]
records_per_csv = 1000

headers = {
    'Content-Type': 'application/json',
    'API-Key': NEW_RELIC_API_KEY,
}

def divide_chunks(l, n):
      
    # looping till length l
    for i in range(0, len(l), n): 
        yield l[i:i + n]

def generate_csv():
    global retry
    global panda_frames_lst
    df = pd.DataFrame()
    timestr = time.strftime("%Y_%m_%d_%H_%M_%S")
    try:
        df=pd.concat(panda_frames_lst, ignore_index=True)
        # Remove the column 'eventType'
        if 'eventType()' in df.columns:
            df.drop(columns=['eventType()'], inplace=True)

        x = list(divide_chunks(df,records_per_csv))
        for idx,i in enumerate(x):
            filename="exported_data_"+timestr+"_"+str(idx)+".csv"
            i.to_csv(filename, encoding='utf-8')
        records_in_csv = len(df. index)
        logging.info("Number of records in CSV "+str(records_in_csv))
    except Exception as e:
        logging.error("Unable to convert dataframe to CSV due to "+str(e))
        retry=True
        return 


def concatenate_messages(blob_data):
    for record in blob_data:
        if log_attribute in record and blob_attribute in record:
            decoded_message = base64.b64decode(record[blob_attribute]).decode('utf-8')
            record[log_attribute] += '' + decoded_message


async def query_blob_total(session, offset, headers):
    json_data = {
        'query': f'''
        {{
            actor {{
                account(id: {NEW_ACCOUNT_ID}) {{
                    nrql(query: "{QUERY} LIMIT {LIMIT} OFFSET {offset}") {{
                        results
                    }}
                }}
            }}
        }}
        ''',
        'variables': '',
    }
    async with session.post('https://api.newrelic.com/graphql', headers=headers, json=json_data, ssl=False) as response:
        response_json = await response.json()
        results = response_json['data']['actor']['account']['nrql']['results']
        if len(results) == 0:
            return None
        else:
            return results

async def query_records():
    offset = 0  # Initial offset
    limit = 20  # Number of records to fetch per request
    tasks = []
    async with aiohttp.ClientSession() as session:
        while True:
            if len(tasks) < num_concurrent_requests:
                tasks.append(query_blob_total(session, offset, headers))
                offset += limit  # Increment the offset to get the next page of results
            if len(tasks) == num_concurrent_requests or offset >= 1000:  # Adjust the condition as needed
                logging.info("Continuing to obtain data from New Relic API, please wait...")
                responses = await asyncio.gather(*tasks)
                tasks = []
                for response_json in responses:
                    if response_json is None:
                        return blob_data  # Exit the loop if there are no more results
                    blob_data.extend(response_json)
    return blob_data

async def main():
    logging.info("Obtaining data from api")
    total_records = await query_records()
    logging.info ("Number of records obtained from api "+ str(len(total_records)))
    temp_df = pd.DataFrame.from_dict(total_records)
    panda_frames_lst.append(temp_df)
    generate_csv()

    
# Run the main function
if __name__ == "__main__":
    asyncio.run(main())