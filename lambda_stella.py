import json
import boto3
import json
import pymongo
import psycopg2
from datetime import datetime

def lambda_handler(event, context):
    s3_bucket_name = 'insert_name'
    s3_client = boto3.client('s3')
    
    for record in event['Records']:
        s3_object_key = record['s3']['object']['key']
        response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_object_key)
        data = response['Body'].read().decode('utf-8')
        
        try:
            # Step 1: Extract data for MongoDB
            mongo_data = {
                "_id": data["member_id"],
                "content_ids": [data["id_tags"]["content_id"]],
                "activity_ids": [data["id_tags"]["activity_id"]],
                "timestamp": datetime.utcnow().isoformat()  # Current timestamp in UTC
            }
            
            
            # Step 2: Extract data for AWS RDS
            rds_member_data = {
                "member_id": data["member_id"],
                "name": data["name"],
                "age": data["age"],
                "accountAge": data["accountAge"],
                "timestamp": datetime.utcnow().isoformat()  # Current timestamp in UTC
            }
            
            rds_content_data = {
                "content_id": data["id_tags"]["content_id"],
                "title": data["content"]["title"],
                "likes": data["content"]["likes"],
                "comments": data["content"]["comments"],
                "company_name": data["content"]["company"].get("name"),
                "yearFounded": data["content"]["company"].get("yearFounded"),
                "lastYearsRevenue": data["content"]["company"].get("lastYearsRevenue"),
                "lastYearSubscribers": data["content"]["company"].get("lastYearSubscribers"),
                "currentSubscribers": data["content"]["company"].get("currentSubscribers")
            }
            
            rds_activity_data = {
                "activity_id": data["id_tags"]["activity_id"],
                "action_id": data["activity"]["action"]["action_id"],
                "type": data["activity"]["action"]["type"],
                "enabled": data["activity"]["action"]["enabled"]
            }
            
            # Step 3: Connect to MongoDB and insert data
            mongo_client = pymongo.MongoClient('insert_client_link')
            mongo_db = mongo_client['client_name']
            mongo_collection = mongo_db['members']  #database
            
            # Step 4: Connect to  RDS PostgreSQL database and insert data
            host = 'endpoint'
            database_name = 'insert_db_name'
            user = 'insert_user_name'
            password = 'insert_password'
            
            try:
                conn = psycopg2.connect(host=host, database=database_name, user=user, password=password)
                cursor = conn.cursor()
                
                # Step 5: Data into MongoDB and AWS RDS
		#into mongo
                mongo_collection.update_one(
                    {"_id": mongo_data["_id"]},
                    {
                        "$addToSet": {
                            "content_ids": {"$each": mongo_data["content_ids"]},
                            "activity_ids": {"$each": mongo_data["activity_ids"]}
                        },
                        "$set": {"timestamp": mongo_data["timestamp"]}
                    },
                    upsert=True
                )
        	
		#into rds
                sql_member = "INSERT INTO members (member_id, name, age, accountAge, timestamp) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql_member, (
                    rds_member_data["member_id"],
                    rds_member_data["name"],
                    rds_member_data["age"],
                    rds_member_data["accountAge"],
                    rds_member_data["timestamp"]
                ))
        
                sql_content = "INSERT INTO content (content_id, title, likes, comments, company_name, yearFounded, lastYearsRevenue, lastYearSubscribers, currentSubscribers) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql_content, (
                    rds_content_data["content_id"],
                    rds_content_data["title"],
                    rds_content_data["likes"],
                    rds_content_data["comments"],
                    rds_content_data["company_name"],
                    rds_content_data["yearFounded"],
                    rds_content_data["lastYearsRevenue"],
                    rds_content_data["lastYearSubscribers"],
                    rds_content_data["currentSubscribers"]
                ))
        
                sql_activity = "INSERT INTO activity (activity_id, action_id, type, enabled) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql_activity, (
                    rds_activity_data["activity_id"],
                    rds_activity_data["action_id"],
                    rds_activity_data["type"],
                    rds_activity_data["enabled"]
                ))
        
                conn.commit()
                cursor.close()
                conn.close()
        
                return {
                    'statusCode': 200,
                    'body': 'Data successfully moved to MongoDB and RDS.'
                }
            except Exception as e:
                return {
                    'statusCode': 500,
                    'body': f'Error: {str(e)}'
                }
            # Step 6: Delete the file from the S3 bucket upon successful commitments
            s3_client.delete_object(Bucket=s3_bucket_name, Key=s3_object_key)
            
        except Exception as e:
            # Handle any errors that occur during data processing or insertion
            
            # You might want to log the error for debugging purposes
            print(f"Error processing data from {s3_object_key}: {str(e)}")
            return {
                'statusCode': 500,
                'body': f'Error processing data from {s3_object_key}'
            }
    
    return {
        'statusCode': 200,
        'body': 'Data forwarding and deletion successful.'
    }