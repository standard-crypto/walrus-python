import pytest
import random
from io import BytesIO

from walrus.client import WalrusClient, WalrusAPIError


@pytest.fixture(scope="session")
def client():
    """Create a WalrusClient instance that's reused across all tests."""
    publisher_url = "https://publisher.testnet.walrus.atalma.io/"
    aggregator_url = "https://aggregator.walrus-testnet.walrus.space/"
    return WalrusClient(publisher_url, aggregator_url)


@pytest.fixture(scope="session")
def string_data():
    """Generate random string data for testing."""
    random_number = random.randint(10000, 99999)
    return f"some string with random number: {random_number}".encode("utf-8")


@pytest.fixture(scope="session")
def file_data(tmp_path_factory):
    """Generate a temporary file with random content for testing."""
    temp_dir = tmp_path_factory.mktemp("test_data")
    file_path = temp_dir / "test_file.txt"

    random_number = random.randint(10000, 99999)
    content = f"file content with random number: {random_number}"

    with open(file_path, "w") as f:
        f.write(content)

    return file_path


@pytest.fixture(scope="session")
def stream_data():
    """Generate a BytesIO stream with random content for testing."""
    random_number = random.randint(10000, 99999)
    content = f"stream content with random number: {random_number}"

    return BytesIO(content.encode("utf-8"))


class BlobFixtureFactory:
    """Helper class to create and validate blob fixtures."""

    @staticmethod
    def create_blob_info(client, data, upload_method, **kwargs):
        """Generic method to upload a blob and return its metadata."""
        response = upload_method(data, **kwargs)

        # Validate response structure
        assert "newlyCreated" in response, "Response missing 'newlyCreated' field"
        blob_object = response["newlyCreated"].get("blobObject")
        assert blob_object is not None, "Response missing 'blobObject' field"

        # Extract and validate IDs
        blob_object_id = blob_object.get("id")
        blob_id = blob_object.get("blobId")
        assert blob_object_id is not None, "Missing blob object id"
        assert blob_id is not None, "Missing blob id"

        return {"object_id": blob_object_id, "blob_id": blob_id}


@pytest.fixture(scope="session")
def blob_info(client, string_data):
    """Upload a blob and return its IDs and original content."""
    info = BlobFixtureFactory.create_blob_info(
        client, string_data, client.put_blob, deletable=True
    )
    info["original_content"] = string_data
    return info


@pytest.fixture(scope="session")
def file_blob_info(client, file_data):
    """Upload a file and return its IDs and content."""
    info = BlobFixtureFactory.create_blob_info(
        client, file_data, client.put_blob_from_file, deletable=True
    )

    # Store original file content
    with open(file_data, "r") as f:
        info["file_content"] = f.read()

    return info


@pytest.fixture(scope="session")
def stream_blob_info(client, stream_data):
    """Upload from a stream and return its IDs and content."""
    # Reset stream position and store content
    stream_data.seek(0)
    stream_content = stream_data.getvalue().decode("utf-8")

    # Reset stream position for upload
    stream_data.seek(0)

    info = BlobFixtureFactory.create_blob_info(
        client, stream_data, client.put_blob_from_stream, deletable=True
    )

    info["stream_content"] = stream_content
    return info


class TestBlobUpload:
    """Tests for blob upload functionality."""

    def test_put_blob(self, blob_info):
        """Verify blob was uploaded correctly."""
        assert blob_info["object_id"], "Blob object ID should exist"
        assert blob_info["blob_id"], "Blob ID should exist"

    def test_put_blob_from_file(self, file_blob_info):
        """Verify file blob was uploaded correctly."""
        assert file_blob_info["object_id"], "Blob object ID should exist"
        assert file_blob_info["blob_id"], "Blob ID should exist"

    def test_put_blob_from_stream(self, stream_blob_info):
        """Verify stream blob was uploaded correctly."""
        assert stream_blob_info["object_id"], "Blob object ID should exist"
        assert stream_blob_info["blob_id"], "Blob ID should exist"


class TestBlobRetrieval:
    """Tests for blob retrieval functionality."""

    def test_get_blob_by_object_id(self, client, blob_info):
        """Test retrieving a blob using its object ID."""
        response = client.get_blob_by_object_id(blob_info["object_id"])
        assert (
            response == blob_info["original_content"]
        ), "Retrieved content doesn't match original"

    def test_get_blob(self, client, blob_info):
        """Test retrieving a blob using its blob ID."""
        response = client.get_blob(blob_info["blob_id"])
        assert (
            response == blob_info["original_content"]
        ), "Retrieved content doesn't match original"

    def test_get_blob_metadata(self, client, blob_info):
        """Test retrieving blob metadata."""
        response = client.get_blob_metadata(blob_info["blob_id"])
        assert response is not None, "Should get a valid response"
        assert response["etag"] == blob_info["blob_id"], "Etag should match blob ID"

    def test_get_blob_as_file(self, client, file_blob_info, tmp_path):
        """Test retrieving a file blob to disk."""
        file_path = tmp_path / "downloaded_file.txt"
        client.get_blob_as_file(file_blob_info["blob_id"], str(file_path))

        with open(file_path, "r") as f:
            downloaded_content = f.read()

        assert (
            downloaded_content == file_blob_info["file_content"]
        ), "Downloaded content doesn't match original"

    def test_get_blob_as_stream(self, client, stream_blob_info):
        """Test retrieving a blob as a stream."""
        response = client.get_blob_as_stream(stream_blob_info["blob_id"])
        assert response is not None, "Should get a valid response"

        content = response.read().decode("utf-8")
        assert (
            content == stream_blob_info["stream_content"]
        ), "Stream content doesn't match original"


class TestErrorHandling:
    """Tests for API error handling."""

    NONEXISTENT_OBJECT_ID = (
        "0x0285f63039460d0640b75f8ca0e6834125db82ee54f768f6a32bb8fa56fe09fe"
    )
    NONEXISTENT_BLOB_ID = "TktRk2y8Ni4vRuJjD8XidZ5qxxWZJjtoz4sU3Xt7cDk"

    def test_get_blob_by_object_id_404(self, client):
        """Test proper error handling when object ID doesn't exist."""
        with pytest.raises(WalrusAPIError) as exc_info:
            client.get_blob_by_object_id(self.NONEXISTENT_OBJECT_ID)

        error = exc_info.value
        assert error.code == 404
        assert error.status == "NOT_FOUND"

    def test_get_blob_404(self, client):
        """Test proper error handling when blob ID doesn't exist."""
        with pytest.raises(WalrusAPIError) as exc_info:
            client.get_blob(self.NONEXISTENT_BLOB_ID)

        error = exc_info.value
        assert error.code == 404
        assert error.status == "NOT_FOUND"

    def test_get_blob_as_stream_404(self, client):
        """Test proper error handling when streaming non-existent blob."""
        with pytest.raises(WalrusAPIError) as exc_info:
            client.get_blob_as_stream(self.NONEXISTENT_BLOB_ID)

        error = exc_info.value
        assert error.code == 404
        assert error.status == "NOT_FOUND"

    def test_get_blob_as_file_404(self, client):
        """Test proper error handling when downloading non-existent blob."""
        with pytest.raises(WalrusAPIError) as exc_info:
            client.get_blob_as_file(self.NONEXISTENT_BLOB_ID, "filename.txt")

        error = exc_info.value
        assert error.code == 404
        assert error.status == "Not Found"
