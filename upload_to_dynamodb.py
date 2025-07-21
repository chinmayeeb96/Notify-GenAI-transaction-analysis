import boto3
import json
import os
from decimal import Decimal
from botocore.exceptions import ClientError

def decimal_default(obj):
    """JSON serializer function that handles Decimal objects"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def convert_floats_to_decimal(obj):
    """Convert float values to Decimal for DynamoDB compatibility"""
    if isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_floats_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj

def upload_user_recommendations_to_dynamodb(user_id, output_data, table_name="UserRecommendations", use_json_string=True):
    """
    Upload user recommendations to DynamoDB table
    
    Args:
        user_id (str): The user ID
        output_data (dict): The complete output data from the pipeline
        table_name (str): DynamoDB table name (default: "UserRecommendations")
        use_json_string (bool): If True, store output as JSON string; if False, store as nested DynamoDB structure
    """
    try:
        # Initialize DynamoDB client
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table(table_name)
        
        if use_json_string:
            # Store as JSON string - much shorter and simpler
            item = {
                'user_id': user_id,
                'output': json.dumps(output_data, default=decimal_default)
            }
        else:
            # Convert floats to Decimal for DynamoDB compatibility (nested structure)
            output_data_decimal = convert_floats_to_decimal(output_data)
            item = {
                'user_id': user_id,
                'output': output_data_decimal
            }
        
        # Put item into DynamoDB
        response = table.put_item(Item=item)
        
        print(f"Successfully uploaded recommendations for user {user_id}")
        print(f"DynamoDB Response: {response.get('ResponseMetadata', {}).get('HTTPStatusCode', 'Unknown')}")
        
        return True
        
    except ClientError as e:
        print(f"Error uploading to DynamoDB: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def upload_all_output_files_to_dynamodb(output_directory="./output", table_name="UserRecommendations", use_json_string=True):
    """
    Upload all user output JSON files to DynamoDB
    
    Args:
        output_directory (str): Directory containing output JSON files
        table_name (str): DynamoDB table name
        use_json_string (bool): If True, store as JSON string; if False, store as nested structure
    """
    success_count = 0
    error_count = 0
    
    # Check if output directory exists
    if not os.path.exists(output_directory):
        print(f"Output directory {output_directory} does not exist")
        return
    
    # Process all JSON files in the output directory
    for filename in os.listdir(output_directory):
        if filename.startswith("output_") and filename.endswith(".json"):
            # Extract user ID from filename (e.g., "output_U1.json" -> "U1")
            user_id = filename.replace("output_", "").replace(".json", "")
            
            try:
                # Read the JSON file
                file_path = os.path.join(output_directory, filename)
                with open(file_path, 'r') as f:
                    output_data = json.load(f)
                
                # Upload to DynamoDB
                if upload_user_recommendations_to_dynamodb(user_id, output_data, table_name, use_json_string):
                    success_count += 1
                    print(f"✓ Uploaded {filename}")
                else:
                    error_count += 1
                    print(f"✗ Failed to upload {filename}")
                    
            except Exception as e:
                error_count += 1
                print(f"✗ Error processing {filename}: {e}")
    
    print(f"\n--- Upload Summary ---")
    print(f"Successfully uploaded: {success_count} files")
    print(f"Errors: {error_count} files")

def upload_single_user_output(user_id, output_file_path, table_name="UserRecommendations", use_json_string=True):
    """
    Upload a single user's output file to DynamoDB
    
    Args:
        user_id (str): The user ID
        output_file_path (str): Path to the JSON output file
        table_name (str): DynamoDB table name
        use_json_string (bool): If True, store as JSON string; if False, store as nested structure
    """
    try:
        with open(output_file_path, 'r') as f:
            output_data = json.load(f)
        
        success = upload_user_recommendations_to_dynamodb(user_id, output_data, table_name, use_json_string)
        
        if success:
            print(f"✓ Successfully uploaded recommendations for user {user_id}")
        else:
            print(f"✗ Failed to upload recommendations for user {user_id}")
            
        return success
        
    except FileNotFoundError:
        print(f"✗ File not found: {output_file_path}")
        return False
    except Exception as e:
        print(f"✗ Error uploading user {user_id}: {e}")
        return False

if __name__ == "__main__":
    # Upload all output files to DynamoDB
    print("Starting upload to DynamoDB...")
    upload_all_output_files_to_dynamodb()
    
    # Example: Upload a specific user's file
    # upload_single_user_output("U1", "./output/output_U1.json")
