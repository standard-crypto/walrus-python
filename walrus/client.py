import os
from typing import Dict, Optional, Any, BinaryIO, IO
import requests
from requests.exceptions import RequestException


class WalrusAPIError(RequestException):
    """Exception raised for errors in the Walrus API responses."""

    def __init__(
        self, code: int, status: str, message: str, details: list, context: str = ""
    ):
        self.code = code
        self.status = status
        self.message = message
        self.details = details
        error_msg = f"{context}: HTTP {code} - {status}: {message}" + (
            f" (Details: {details})" if details else ""
        )
        super().__init__(error_msg)

    def __str__(self) -> str:
        return f"HTTP {self.code} - {self.status}: {self.message}" + (
            f" (Details: {self.details})" if self.details else ""
        )


class WalrusClient:
    """
    Client for interacting with the Walrus API.

    Provides methods for uploading and retrieving binary blobs from publisher and aggregator endpoints.
    """

    def __init__(
        self, publisher_base_url: str, aggregator_base_url: str, timeout: int = 30
    ):
        """
        Initialize the Walrus client.

        Args:
            publisher_base_url: Base URL for the publisher service
            aggregator_base_url: Base URL for the aggregator service
            timeout: Request timeout in seconds
        """
        self.publisher_base_url = publisher_base_url.rstrip("/")
        self.aggregator_base_url = aggregator_base_url.rstrip("/")
        self.timeout = timeout

    def put_blob(
        self,
        data: bytes,
        encoding_type: Optional[str] = None,
        epochs: Optional[int] = None,
        deletable: Optional[bool] = None,
        send_object_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Upload binary data (a blob) to a publisher.

        Args:
            data: Binary data to upload
            encoding_type: The encoding type to use for the blob
            epochs: Number of epochs ahead of the current one to store the blob
            deletable: If true, creates a deletable blob instead of a permanent one
            send_object_to: If specified, sends the Blob object to this Sui address

        Returns:
            JSON response from the server

        Raises:
            WalrusAPIError: If the API request fails
        """
        url = f"{self.publisher_base_url}/v1/blobs"
        headers = {"Content-Type": "application/octet-stream"}
        params = self._build_query_params(
            encoding_type, epochs, deletable, send_object_to
        )

        try:
            response = requests.put(
                url, data=data, headers=headers, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            self._handle_request_error(e, "Error uploading blob")

    def put_blob_from_file(
        self,
        file_path: str,
        encoding_type: Optional[str] = None,
        epochs: Optional[int] = None,
        deletable: Optional[bool] = None,
        send_object_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Upload a blob from a file to the publisher.

        Args:
            file_path: Path to the file to upload
            encoding_type: The encoding type to use for the blob
            epochs: Number of epochs ahead of the current one to store the blob
            deletable: If true, creates a deletable blob instead of a permanent one
            send_object_to: If specified, sends the Blob object to this Sui address

        Returns:
            JSON response from the server

        Raises:
            FileNotFoundError: If the file does not exist
            WalrusAPIError: If the API request fails
        """
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, "rb") as file:
            data = file.read()

        return self.put_blob(data, encoding_type, epochs, deletable, send_object_to)

    def put_blob_from_stream(
        self,
        stream: BinaryIO,
        encoding_type: Optional[str] = None,
        epochs: Optional[int] = None,
        deletable: Optional[bool] = None,
        send_object_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Upload a blob from a binary stream to the publisher.

        Args:
            stream: Binary stream to upload (must be in binary mode)
            encoding_type: The encoding type to use for the blob
            epochs: Number of epochs ahead of the current one to store the blob
            deletable: If true, creates a deletable blob instead of a permanent one
            send_object_to: If specified, sends the Blob object to this Sui address

        Returns:
            JSON response from the server

        Raises:
            ValueError: If the stream is not readable
            WalrusAPIError: If the API request fails
        """
        if not stream.readable():
            raise ValueError("Provided stream is not readable")

        url = f"{self.publisher_base_url}/v1/blobs"
        headers = {"Content-Type": "application/octet-stream"}
        params = self._build_query_params(
            encoding_type, epochs, deletable, send_object_to
        )

        try:
            response = requests.put(
                url, data=stream, headers=headers, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            self._handle_request_error(e, "Error uploading blob from stream")

    def get_blob_by_object_id(self, object_id: str) -> bytes:
        """
        Retrieve a blob from the aggregator by its object ID.

        Args:
            object_id: The object ID of the blob

        Returns:
            Binary content of the blob

        Raises:
            WalrusAPIError: If the API request fails
        """
        url = f"{self.aggregator_base_url}/v1/blobs/by-object-id/{object_id}"

        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.content
        except RequestException as e:
            self._handle_request_error(
                e, f"Error retrieving blob by object ID: {object_id}"
            )

    def get_blob(self, blob_id: str) -> bytes:
        """
        Retrieve a blob from the aggregator by its blob ID.

        Args:
            blob_id: The blob ID

        Returns:
            Binary content of the blob

        Raises:
            WalrusAPIError: If the API request fails
        """
        url = f"{self.aggregator_base_url}/v1/blobs/{blob_id}"

        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.content
        except RequestException as e:
            self._handle_request_error(
                e, f"Error retrieving blob by blob ID: {blob_id}"
            )

    def get_blob_as_stream(self, blob_id: str) -> IO[bytes]:
        """
        Retrieve a blob from the aggregator as a stream by its blob ID.

        Args:
            blob_id: The blob ID

        Returns:
            A file-like object (stream) for reading the blob data

        Raises:
            WalrusAPIError: If the API request fails
        """
        url = f"{self.aggregator_base_url}/v1/blobs/{blob_id}"
        try:
            response = requests.get(url, stream=True, timeout=self.timeout)
            response.raise_for_status()
            return response.raw
        except RequestException as e:
            self._handle_request_error(
                e, f"Error retrieving blob as stream by blob ID: {blob_id}"
            )

    def get_blob_as_file(self, blob_id: str, file_path: str) -> None:
        """
        Retrieve a blob from the aggregator by its blob ID and save it to a file.

        Args:
            blob_id: The blob ID
            file_path: The destination file path where the blob will be saved

        Raises:
            WalrusAPIError: If the API request fails
        """
        url = f"{self.aggregator_base_url}/v1/blobs/{blob_id}"
        try:
            with requests.get(url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
        except RequestException as e:
            self._handle_request_error(
                e, f"Error retrieving blob as file by blob ID: {blob_id}"
            )

    def get_blob_metadata(self, blob_id: str) -> Dict[str, str]:
        """
        Retrieve metadata for a blob from the aggregator by making a HEAD request.

        Args:
            blob_id: The blob ID

        Returns:
            Dictionary containing the response headers

        Raises:
            WalrusAPIError: If the API request fails
        """
        url = f"{self.aggregator_base_url}/v1/blobs/{blob_id}"

        try:
            response = requests.head(url, timeout=self.timeout)
            response.raise_for_status()
            return dict(response.headers)
        except RequestException as e:
            self._handle_request_error(
                e, f"Error retrieving metadata for blob ID: {blob_id}"
            )

    def _build_query_params(
        self,
        encoding_type: Optional[str] = None,
        epochs: Optional[int] = None,
        deletable: Optional[bool] = None,
        send_object_to: Optional[str] = None,
    ) -> Dict[str, str]:
        """Build query parameters for blob upload requests."""
        params = {}
        if encoding_type is not None:
            params["encoding_type"] = encoding_type
        if epochs is not None:
            params["epochs"] = str(epochs)
        if deletable is not None:
            params["deletable"] = "true" if deletable else "false"
        if send_object_to is not None:
            params["send_object_to"] = send_object_to
        return params

    def _handle_request_error(self, exception: RequestException, context: str) -> None:
        """Handle request exceptions by extracting structured error information."""
        if hasattr(exception, "response") and exception.response is not None:
            try:
                if exception.response.content:
                    error_json = exception.response.json()
                    if isinstance(error_json, dict) and "error" in error_json:
                        err = error_json["error"]
                        code = err.get("code", exception.response.status_code)
                        status = err.get("status", "UNKNOWN")
                        message = err.get("message", "")
                        details = err.get("details", [])
                        raise WalrusAPIError(
                            code, status, message, details, context=context
                        ) from exception

                # Fall back to HTTP response info
                code = exception.response.status_code
                status = exception.response.reason or "UNKNOWN"
                message = f"HTTP {code}: {status}"
                raise WalrusAPIError(
                    code, status, message, [], context=context
                ) from exception

            except (ValueError, requests.exceptions.JSONDecodeError):
                code = exception.response.status_code
                status = exception.response.reason or "UNKNOWN"
                message = f"HTTP {code}: {status}"
                raise WalrusAPIError(
                    code, status, message, [], context=context
                ) from exception
        else:
            # No response available - network error, timeout, etc.
            raise WalrusAPIError(
                500, "REQUEST_FAILED", str(exception), [], context=context
            ) from exception
