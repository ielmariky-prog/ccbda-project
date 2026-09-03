import boto3
iam = boto3.client("iam")
roles = iam.list_roles()["Roles"]
for r in roles:
    print(r["RoleName"], "->", r["Arn"])
