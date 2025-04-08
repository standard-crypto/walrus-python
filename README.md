# Walrus Python SDK 

The walrus-python SDK provides a Python client for interacting with the Walrus HTTP API. Walrus is a decentralized storage system built on the Sui blockchain, allowing you to store and retrieve blobs efficiently.

[![PyPI](https://img.shields.io/pypi/v/walrus-python.svg)](https://pypi.org/project/walrus-python/)
[![License](https://img.shields.io/pypi/l/walrus-python.svg)](https://pypi.org/project/walrus-python/)
![Tests](https://github.com/standard-crypto/walrus-python/actions/workflows/test.yml/badge.svg)

## Installation

```commandline
pip install walrus-python
```

## Usage

### Initializing the Client

```python
from walrus import WalrusClient

publisher_url = "https://publisher.walrus-testnet.walrus.space"
aggregator_url = "https://aggregator.walrus-testnet.walrus.space"

client = WalrusClient(publisher_base_url=publisher_url, aggregator_base_url=aggregator_url)
```


### Uploading a Blob

#### Available Parameters

- `encoding_type` (*Optional[str]*): Specifies the encoding used for the blob.

- `epochs` (*Optional[int]*): Number of epochs ahead of the current one to store the blob.

- `deletable` (*Optional[bool]*): Determines whether the blob can be deleted later (True) or is permanent (False).

- `send_object_to` (*Optional[str]*): If provided, sends the Blob object to the specified Sui address.

#### From Bytes

```python
blob_data = b"Hello Walrus!"
response = client.put_blob(data=blob_data)
print(response)
```

#### From a File

```python
file_path = "path/to/your/file.txt"
response = client.put_blob_from_file(file_path)
print(response)
```

#### From a Binary Stream

```python
url = "https://example.com/stream"
with requests.get(url, stream=True) as response:
    result = client.put_blob_from_stream(response.raw)
    print(result)
```

### Retrieving a Blob

#### By Blob ID

```python
blob_id = "your-blob-id"
blob_content = client.get_blob(blob_id)
print(blob_content)
```

#### By Object ID

```python
object_id = "your-object-id"
blob_content = client.get_blob_by_object_id(object_id)
print(blob_content)
```

#### As a File

```python
blob_id = "your-blob-id"
destination_path = "downloaded_blob.jpg"
client.get_blob_as_file(blob_id, destination_path)
print(f"Blob saved to {destination_path}")
```

#### As a Stream

```python
blob_id = "your-blob-id"
stream = client.get_blob_as_stream(blob_id)
with open("streamed_blob.bin", "wb") as f:
    f.write(stream.read())
```

#### Retrieving Blob Metadata

```python
blob_id = "your-blob-id"
metadata = client.get_blob_metadata(blob_id)
print(metadata)
```

### Error Handling

WalrusAPIError provides structured error information:

```python
try:
    client.get_blob("non-existent-id")
except WalrusAPIError as e:
    print(e)
```

### Contributing

Contributions are welcome! Please submit a pull request or open an issue for any suggestions, improvements, or bug reports.
