from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids,get_video_details,save_to_json
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
#define the local timezone
local_tz = pendulum.timezone("Asia/Kolkata")

#Default Args
default_args ={
    "owner" : "dataengineers",
    "depends_on_past": False,
    "email_on_failure" : False,
    "email_on_retry"  : False,
    "email": "praveenyennam@gmail.com",
    # "retries" : 1
    # "retry_delay" : timedelta(minutes =5),
    "max_active_runs" : 1,
    "dagrun_timeout":timedelta(hours =1),
    "start_date" : datetime(2025,12,21, tzinfo=local_tz),
    #"end_date" : datetime(2026,01,01, tzinfo = local_tz),
}

with DAG(
    dag_id = 'produce_json', #as we extract data from youtube api and store in json format in raw layer
    default_args = default_args,
    description = "DAG to produce JSON file with raw data",
    schedule = "0 14 * * *", #cron syntax for scheduling DAG at 2 PM daily
    catchup = False # tells airflow not catchup missed DAGS from the past
) as dag:

    #define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    data_extract = get_video_details(video_ids)
    save_to_json_task = save_to_json(data_extract)
    
    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )
    #define dependencies
    playlist_id >> video_ids >> data_extract >> save_to_json_task