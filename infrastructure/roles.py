import boto3

def deploy_roles():
    client = boto3.client('iam')

    glue_role_response = client.create_role(
        Path='/pipedream/',
        RoleName='PipedreamGlueRole',
        AssumeRolePolicyDocument="""{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "glue.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }""",
        Description='Pipedrean Glue Service Role',
        # MaxSessionDuration=123,
        # PermissionsBoundary='string',
        Tags=[
            {
                'Key': 'CreatedBy',
                'Value': 'Pipedream'
            },
        ]
    )

    glue_policy_response = client.attach_role_policy(
        RoleName='PipedreamGlueRole',
        PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
    )


    step_functions_role_response = client.create_role(
        Path='/pipedream/',
        RoleName='PipedreamStepFunctionsRole',
        AssumeRolePolicyDocument="""{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {
                    "Service": "states.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }""",
        Description='Pipedrean Step Functions Role',
        # MaxSessionDuration=123,
        # PermissionsBoundary='string',
        Tags=[
            {
                'Key': 'CreatedBy',
                'Value': 'Pipedream'
            },
        ]
    )

    step_functions_lambda_policy_response = client.attach_role_policy(
        RoleName='PipedreamStepFunctionsRole',
        PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaRole'
    )

    step_functions_glue_policy_response = client.create_policy(
        PolicyName='PipedreamStepFunctionsPolicy',
        Path='/pipedream/',
        PolicyDocument="""{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor0",
                "Effect": "Allow",
                "Action": "glue:*",
                "Resource": "*"
            }
        ]
    }""",
        Description='Allows step functions to execute glue jobs',
        Tags=[
            {
                'Key': 'CreatedBy',
                'Value': 'Pipedream'
            },
        ]
    )

    print(step_functions_glue_policy_response)
    step_functions_glue_policy_attachment_response = client.attach_role_policy(
        RoleName='PipedreamStepFunctionsRole',
        PolicyArn=step_functions_glue_policy_response["Policy"]["Arn"]
    )