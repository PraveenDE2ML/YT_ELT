#convert ISO 8601 duration to proper time type and add Video_Type column
from datetime import datetime, timedelta

from sqlalchemy import values
def parse_duration(duration_str):
    """Parse a duration string from ISO 8601 format to proper time type.
    Example: 'PT1H2M10S' -> timedelta(hours=1, minutes=2, seconds=10)
    """
    duration_str = duration_str.replace("P","").replace("T","")
    components =['D', 'H', 'M', 'S']
    values = {'D': 0, 'H': 0, 'M': 0, 'S': 0}

    for component in components:
        if component in duration_str:
            part, duration_str = duration_str.split(component)
            values[component] = int(part) # convert  part to integer
        
    total_duration = timedelta( days=values['D'], hours=values['H'], minutes=values['M'], seconds=values['S'])
    return total_duration

def transform_data(row):
    duration_td = parse_duration(row['Duration'])
    row['Duration'] = (datetime.min + duration_td).time()  # Convert timedelta to time object
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"
    return row

