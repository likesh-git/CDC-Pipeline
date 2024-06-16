import base64
import json

def lambda_handler(event, context):
    output_records = []

    for record in event['records']:
        try: 
            # Decode the input data from base64
            payload = base64.b64decode(record['data']).decode('utf-8')
            payload_json = json.loads(payload)

            # Access the data in the 'dynamodb' key
            dynamodb_data = payload_json['dynamodb']
            new_image = dynamodb_data['NewImage']

            # Extract required fields from new image
            transformed_data = {
                'order_id': new_image['orderid']['S'],
                'product_name': new_image['product_name']['S'],
                'quantity': int(new_image['quantity']['N']),
                'price': float(new_image['price']['N'])
            }

            # Convert the transformed data to JSON string and then encode it as base64
            transformed_data_str = json.dumps(transformed_data) + '\n'
            transformed_data_encoded = base64.b64encode(transformed_data_str.encode('utf-8')).decode('utf-8')

            # Append the transformed record to the output using 'recordId' from the event
            output_records.append({
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': transformed_data_encoded
            })
            print("Transformation successful for recordId:", record['recordId'])

        except Exception as e:
            # If there is any error with processing the record, mark it as ProcessingFailed but still return the recordId
            print('Exception occurred for recordId:', record['recordId'], '; Error:', str(e))
            output_records.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data'] # Simply pass the original data back
            })

    return {
        'records': output_records
    }
