This project automatically indexes metadata and content information from newly uploaded blobs into Cosmos DB using Event Grid and Azure Functions.

Features

Triggered by Event Grid when a blob is created

Reads blob metadata and content

Extracts title (first H1 or first line)

Counts number of words for text files

Inserts an indexed record into Cosmos DB Documents container

Handles duplicates safely using id=blobName

Supports text detection using MIME type

Captured Metadata

Blob name

URL

Size

Content type

Title extracted

Word count

Uploaded timestamp
