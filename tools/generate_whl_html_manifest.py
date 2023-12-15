from __future__ import annotations

import boto3

s3 = boto3.client("s3")

response = s3.list_objects_v2(Bucket="github-actions-artifacts-bucket")

all_objects = response["Contents"]

all_wheels = [obj["Key"] for obj in all_objects if obj["Key"].endswith(".whl")]

html_href = []

for key in all_wheels:
    name = key.split("/")[-1]
    link = key.replace("+", "%2B")
    href = f"    <a href='../{link}'>{name} </a>"
    html_href.append(href)
    html_href.append("    <br />")

html_href.pop()

all_href = "\n".join(h for h in html_href)

manifest = f"""<html>
<head>
    <title>Links</title>
</head>
<body>
    <h1>Links</h1>
    {all_href}
</body>
</html>"""


with open("index.html", "w") as f:
    f.write(manifest)

s3.upload_file(
    "index.html",
    Bucket="github-actions-artifacts-bucket",
    Key="getdaft/index.html",
    ExtraArgs={"ACL": "public-read", "ContentType": "text/html"},
)

print("uploaded:")
print(manifest)
