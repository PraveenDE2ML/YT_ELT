import logging

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur,conn, schema, row):
    try:
        if(schema == "staging"):
            video_id = "videoId"
            insert_sql = f"""
                INSERT INTO {schema}.{table} ("Video_Id", "Video_Title", "Upload_Date", "Duration", "view_count", "like_count", "comment_count")
                VALUES (%(videoId)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)
                ON CONFLICT ("Video_Id") DO NOTHING;
            """
            cur.execute(insert_sql, row)
        else:
            video_id = "Video_Id"
            insert_sql = f"""
                INSERT INTO {schema}.{table} ("Video_Id", "Video_Title", "Upload_Date", "Duration", "view_count", "like_count", "comment_count")
                VALUES (%(Video_Id)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(view_count)s, %(like_count)s, %(comment_count)s)
                ON CONFLICT ("Video_Id") DO NOTHING;
            """
            cur.execute(insert_sql, row)
        conn.commit()
        logger.info(f"Inserted row with Video_ID: {row[video_id]}")
    except Exception as e:
        logger.error(f"Error inserting row for video ID {row[video_id]}: {e}")
        raise e

def update_rows(cur, conn, schema, row):
    try:
        if schema == "staging":
            Video_Id = "videoId"
            upload_date = "publishedAt"
            video_title = "title"
            view_count = "viewCount"
            like_count = "likeCount"
            comment_count = "commentCount"
        else:
            Video_Id = "Video_Id"
            upload_date = "Upload_Date"
            video_title = "Video_Title"
            view_count = "view_count"
            like_count = "like_count"
            comment_count = "comment_count"
        update_sql = f""" update {schema}.{table}
            set "Video_Title" = %({video_title})s,
                "view_count" = %({view_count})s,
                "like_count" = %({like_count})s,
                "comment_count" = %({comment_count})s
            where "Video_Id" = %({Video_Id})s and "Upload_Date" = %({upload_date})s;
        """
        cur.execute(update_sql, row)
        conn.commit()
        logger.info(f"Updated row with Video_ID: {row[Video_Id]}")
    except Exception as e:
        logger.error(f"Error updating row for video ID {row[Video_Id]}: {e}")
        raise e
def delete_rows(cur, conn, schema, ids_to_delete):
    try:
        # Ensure ids_to_delete is always a list
        if isinstance(ids_to_delete, str):
            ids_to_delete = [ids_to_delete]

        # Build placeholders for each ID
        placeholders = ', '.join(['%s'] * len(ids_to_delete))
        delete_sql = f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_Id" IN ({placeholders});
        """
        cur.execute(delete_sql, ids_to_delete)  # pass list directly
        conn.commit()
        logger.info(f"Deleted rows with Video IDs: {ids_to_delete}")
    except Exception as e:
        logger.error(f"Error deleting rows for Video IDs {ids_to_delete}: {e}")
        raise e
