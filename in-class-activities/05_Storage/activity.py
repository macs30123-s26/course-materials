import boto3
import json

# Lambda client
aws_lambda = boto3.client('lambda')

# Access our class IAM role, which allows Lambda
# to interact with other AWS resources
iam_client = boto3.client('iam')
role = iam_client.get_role(RoleName='LabRole')

# Open our Zipped directory
with open('activity.zip', 'rb') as f:
    lambda_zip = f.read()

try:
    # If function hasn't yet been created, create it
    response = aws_lambda.create_function(
        FunctionName='activity',
        Runtime='python3.10',
        Role=role['Role']['Arn'],
        Handler='lambda_function.lambda_handler',
        Code=dict(ZipFile=lambda_zip),
        MemorySize=128,
        Timeout=5
    )
except aws_lambda.exceptions.ResourceConflictException:
    # If function already exists, update it based on zip
    # file contents
    response = aws_lambda.update_function_code(
        FunctionName='activity',
        ZipFile=lambda_zip
    )
    # After code updated, update config as well if necessary
    waiter = aws_lambda.get_waiter('function_updated')
    waiter.wait(FunctionName='activity')
    response_config = aws_lambda.update_function_configuration(
        FunctionName='activity',
        MemorySize=128
    )

# Pass in any test data (function doesn't actually process 'event')
test_data = {'key1': 1, 'key2': 2}

# run synchronously:
r = aws_lambda.invoke(FunctionName='activity',
                      InvocationType='RequestResponse',
                      Payload=json.dumps(test_data))
payload = json.loads(r['Payload'].read())
print(payload) # print out response
