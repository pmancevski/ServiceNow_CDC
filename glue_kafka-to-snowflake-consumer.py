import sys
import json
from awsglue.utils import getResolvedOptions
from kafka import KafkaConsumer
import snowflake.connector

print("Init args", flush=True)

args = getResolvedOptions(sys.argv, [
    'KAFKA_TOPIC',
    'KAFKA_BOOTSTRAP_SERVERS',
    'KAFKA_API_KEY',
    'KAFKA_API_SECRET',
    'SNOWFLAKE_ACCOUNT',
    'SNOWFLAKE_USER',
    'SNOWFLAKE_PASSWORD',
    'SNOWFLAKE_WAREHOUSE',
    'SNOWFLAKE_DATABASE',
    'SNOWFLAKE_SCHEMA'
])

print("Args loaded:", args, flush=True)

def main():
    print("Glue job started. create consumer")
    count = 0
    max_messages = 1
    
    consumer = KafkaConsumer(
        args['KAFKA_TOPIC'],
        bootstrap_servers=args['KAFKA_BOOTSTRAP_SERVERS'],
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username=args['KAFKA_API_KEY'],
        sasl_plain_password=args['KAFKA_API_SECRET'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='glue-consumer-group-v4'
    )
    
    print("consumer block initialized", flush=True)
    
    print("create conn to snowflake", flush=True)
    
    conn = snowflake.connector.connect(
        account=args['SNOWFLAKE_ACCOUNT'],
        user=args['SNOWFLAKE_USER'],
        password=args['SNOWFLAKE_PASSWORD'],
        warehouse=args['SNOWFLAKE_WAREHOUSE'],
        database=args['SNOWFLAKE_DATABASE'],
        schema=args['SNOWFLAKE_SCHEMA']
    )
    
    cursor = conn.cursor()
    total = 0
    
    print("MY TEST TEST 01", flush=True)
    
    for msg in consumer:
        data = msg.value
        print(json.dumps(data, indent=2), flush=True)
        
        cursor.execute("""
            INSERT INTO CHANGE_LOG (created_on, table_name, record_id, field_name, old_value, new_value, changed_by, audit_id, ticket_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data.get('created_on'),
            data.get('table_name'),
            data.get('record_id'),
            data.get('field_name'),
            data.get('old_value'),
            data.get('new_value'),
            data.get('changed_by'),
            data.get('audit_id'),
            data.get('ticket_number')
        ))
        conn.commit()
        total += 1
        print(f"Inserted {total} records", flush=True)
        count += 1
        if count >= max_messages:
            print(f"Processed {max_messages} messages, stopping", flush=True)
            break
        
    consumer.close()  # ← Gracefully close connection
    
    cursor.close()
    conn.close()
    print(f"Total inserted: {total}", flush=True)
    print("Glue job finished", flush=True)

if __name__ == "__main__":
    main()
