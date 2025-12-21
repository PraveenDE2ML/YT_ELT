import requests
import json
# import os

from datetime import date
# from dotenv import load_dotenv
# load_dotenv(dotenv_path="./.env") #set the dotenv path
# API_KEY = os.getenv("API_KEY")
# CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")

from airflow.decorators import task, dag
from airflow.variables import Variable

API_KEY = Variable.get("API_KEY ")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")

maxresults = 50

@task
def get_playlist_id():
    try: 
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}'

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        #print(json.dumps(data, indent = 4))

        channel_items = data["items"][0]
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        #print(channel_playlistId)
        return channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e

@task
def get_video_ids(playlistId):  
    
    video_ids = []

    pageToken = None

    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxresults}&playlistId={playlistId}&key={API_KEY}'

    try:
        while True: #Infinite Loop
            
            if pageToken:
                url = f'{base_url}&pageToken={pageToken}'    
            else:
                url = base_url #first page doesn't have pageToken
                #For Testing: #print(url)

            response = requests.get(url) # If the response is successful, no exception is raised
            response.raise_for_status()

            data = response.json()
            # For testing: #print(json.dumps(data, indent = 4))

            for item in data.get("items",[]):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

            pageToken = data.get("nextPageToken")
            if not pageToken:
                break
        return video_ids
        
    except requests.exceptions.RequestException as e:
        raise e
   

@task
def get_video_details(video_ids):
    extracted_data = []

    def batch_list(video_id_lst, batch_size):
        for i in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[i:i+batch_size] #get the sub-list of video_ids equal to batch size
    try:
        for batch in batch_list(video_ids, maxresults):
            video_ids_str = ",".join(batch)
            url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]
                video_data = {
                    "videoId" : video_id,    
                    "title": snippet.get("title"),
                    "publishedAt": snippet.get("publishedAt"),
                    "duration": contentDetails.get("duration"),
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None)
                }
                extracted_data.append(video_data)
        #print(json.dumps(extracted_data, indent=4))
        return extracted_data
        
    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    file_path = f"./data/video_stats_{date.today()}.json" #file path with current date
    
    #context manager to handle file operations like open,close and write
    with open(file_path, "w", encoding="utf-8") as json_file: #open file in write mode with utf-8 encoding, 'w' indicates write mode
        json.dump(extracted_data, json_file, indent=4,ensure_ascii=False) #dump data to json file with indentation of 4 spaces

if __name__ == "__main__":
    playlistId = get_playlist_id()
    video_id_list = get_video_ids(playlistId)
    video_data = get_video_details(video_id_list)
    save_to_json(video_data)