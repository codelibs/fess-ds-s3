Amazon S3 Data Store for Fess
[![Java CI with Maven](https://github.com/codelibs/fess-ds-s3/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-s3/actions/workflows/maven.yml)
==========================

## Overview

Amazon S3 Data Store is an extension for Fess Data Store Crawling.

## Download

See [Maven Repository](http://central.maven.org/maven2/org/codelibs/fess/fess-ds-s3/).

## Installation 

See [Plugin](https://fess.codelibs.org/13.3/admin/plugin-guide.html) of Administration guide.

## Getting Started

### Parameters

```
region=...
access_key_id=...
secret_key=...
```

| Key | Value |
| --- | --- |
| *region* | The region of the S3 buckets (eg: `ap-northeast-1`) |
| *access_key_id* | AWS Access Key ID |
| *secret_key* | AWS Secret Key |

### Scripts

```
url=object.url
title=object.key
content=object.contents
mimetype=object.mimetype
filetype=object.filetype
filename=object.filename
content_length=object.size
last_modified=object.last_modified
```

| Key | Value |
| --- | --- |
| *object.url* | The URL of the S3 object. |
| *object.management_url* | The management URL of the S3 Object. |
| *object.key* | The key of the S3 object. |
| *object.e_tag* | The ETag of the S3 object. |
| *object.contents* | The text content of the S3 object. |
| *object.mimetype* | The mimetype of the S3 object. |
| *object.filetype* |  The filetype of the S3 object. |
| *object.filename* | The file name of the S3 object. |
| *object.size* | The size of the S3 object. |
| *object.last_modified* | The last time the S3 object was modified. |
| *object.owner_id* | The owner ID of the S3 object. |
| *object.owner_display_name* | The display name of the S3 object's owner. |
| *object.bucket_name* | The bucket name of the S3 object. |
| *object.creation_date* | The time when the bucket created. |

