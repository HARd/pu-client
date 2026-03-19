import base64
import hashlib
import os
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import quote, urlencode

import requests

class BackblazeB2Client:
    def __init__(self) -> None:
        self.account_id = None
        self.authorization_token = None
        self.api_url = None
        self.download_url = None

    def authorize(self, key_id: str, application_key: str) -> None:
        credentials = f"{key_id}:{application_key}".encode("utf-8")
        auth_header = base64.b64encode(credentials).decode("utf-8")

        response = requests.get(
            "https://api.backblazeb2.com/b2api/v2/b2_authorize_account",
            headers={"Authorization": f"Basic {auth_header}"},
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        self.account_id = data["accountId"]
        self.authorization_token = data["authorizationToken"]
        self.api_url = data["apiUrl"]
        self.download_url = data["downloadUrl"]

    def _require_auth(self) -> None:
        if not self.authorization_token or not self.api_url:
            raise RuntimeError("Client is not authorized.")

    def get_upload_url(self, bucket_id: str) -> dict:
        self._require_auth()
        response = requests.post(
            f"{self.api_url}/b2api/v2/b2_get_upload_url",
            headers={"Authorization": self.authorization_token},
            json={"bucketId": bucket_id},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def upload_file(
        self,
        bucket_id: str,
        local_path: str,
        file_name_in_bucket: str,
        progress_cb: Optional[Callable[[str, int, int], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        wait_if_paused: Optional[Callable[[], None]] = None,
    ) -> dict:
        upload_info = self.get_upload_url(bucket_id)
        upload_url = upload_info["uploadUrl"]
        upload_auth_token = upload_info["authorizationToken"]

        total_size = os.path.getsize(local_path)
        sha1 = self._compute_file_sha1(local_path, total_size, progress_cb, should_stop, wait_if_paused)

        headers = {
            "Authorization": upload_auth_token,
            "X-Bz-File-Name": quote(file_name_in_bucket, safe="/"),
            "Content-Type": "b2/x-auto",
            "Content-Length": str(total_size),
            "X-Bz-Content-Sha1": sha1,
        }

        with open(local_path, "rb") as f:
            stream = UploadProgressReader(f, total_size, progress_cb, should_stop, wait_if_paused)
            response = requests.post(upload_url, headers=headers, data=stream, timeout=120)

        if response.status_code >= 400:
            try:
                details = response.json()
            except Exception:
                details = response.text
            raise RuntimeError(f"Upload failed ({response.status_code}): {details}")

        return response.json()

    def _compute_file_sha1(
        self,
        local_path: str,
        total_size: int,
        progress_cb: Optional[Callable[[str, int, int], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        wait_if_paused: Optional[Callable[[], None]] = None,
    ) -> str:
        hasher = hashlib.sha1()
        processed = 0
        chunk_size = 1024 * 1024

        with open(local_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                if should_stop and should_stop():
                    raise RuntimeError("Transfer stopped by user.")
                if wait_if_paused:
                    wait_if_paused()
                hasher.update(chunk)
                processed += len(chunk)
                if progress_cb:
                    progress_cb("hash", processed, total_size)

        if progress_cb:
            progress_cb("hash", total_size, total_size)

        return hasher.hexdigest()

    def list_files(self, bucket_id: str, prefix: str = "", max_count: int = 1000) -> List[Dict]:
        self._require_auth()

        payload = {
            "bucketId": bucket_id,
            "maxFileCount": max_count,
        }
        if prefix:
            payload["prefix"] = prefix

        response = requests.post(
            f"{self.api_url}/b2api/v2/b2_list_file_names",
            headers={"Authorization": self.authorization_token},
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("files", [])

    def list_files_all(self, bucket_id: str, prefix: str = "", max_count: int = 1000) -> List[Dict]:
        self._require_auth()
        all_files: List[Dict] = []
        next_file_name = None

        while True:
            payload = {
                "bucketId": bucket_id,
                "maxFileCount": max_count,
            }
            if prefix:
                payload["prefix"] = prefix
            if next_file_name:
                payload["startFileName"] = next_file_name

            response = requests.post(
                f"{self.api_url}/b2api/v2/b2_list_file_names",
                headers={"Authorization": self.authorization_token},
                json=payload,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            all_files.extend(data.get("files", []))
            next_file_name = data.get("nextFileName")
            if not next_file_name:
                break

        return all_files

    def get_download_authorization(self, bucket_id: str, file_name: str, valid_seconds: int) -> str:
        self._require_auth()

        response = requests.post(
            f"{self.api_url}/b2api/v2/b2_get_download_authorization",
            headers={"Authorization": self.authorization_token},
            json={
                "bucketId": bucket_id,
                "fileNamePrefix": file_name,
                "validDurationInSeconds": valid_seconds,
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["authorizationToken"]

    def make_direct_url(self, bucket_name: str, file_name: str, auth_token: Optional[str] = None) -> str:
        if not self.download_url:
            raise RuntimeError("Missing download URL. Authorize first.")

        encoded_file_name = quote(file_name, safe="/")
        url = f"{self.download_url}/file/{bucket_name}/{encoded_file_name}"

        if auth_token:
            return f"{url}?{urlencode({'Authorization': auth_token})}"
        return url

    def download_file(
        self,
        bucket_name: str,
        file_name: str,
        target_path: str,
        progress_cb: Optional[Callable[[int, int], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        wait_if_paused: Optional[Callable[[], None]] = None,
    ) -> None:
        self._require_auth()
        url = self.make_direct_url(bucket_name, file_name)
        headers = {"Authorization": self.authorization_token}

        response = requests.get(url, headers=headers, stream=True, timeout=120)
        if response.status_code >= 400:
            try:
                details = response.json()
            except Exception:
                details = response.text
            raise RuntimeError(f"Download failed ({response.status_code}): {details}")

        total = int(response.headers.get("Content-Length", "0"))
        downloaded = 0
        chunk_size = 1024 * 256

        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                if should_stop and should_stop():
                    raise RuntimeError("Transfer stopped by user.")
                if wait_if_paused:
                    wait_if_paused()
                f.write(chunk)
                downloaded += len(chunk)
                if progress_cb:
                    progress_cb(downloaded, total)

        if progress_cb:
            progress_cb(downloaded, total)


class UploadProgressReader:
    def __init__(
        self,
        file_obj,
        total_size: int,
        progress_cb: Optional[Callable[[str, int, int], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        wait_if_paused: Optional[Callable[[], None]] = None,
    ) -> None:
        self.file_obj = file_obj
        self.total_size = total_size
        self.sent = 0
        self.progress_cb = progress_cb
        self.should_stop = should_stop
        self.wait_if_paused = wait_if_paused

    def __len__(self) -> int:
        return self.total_size

    def tell(self) -> int:
        return self.sent

    def seek(self, offset: int, whence: int = 0) -> int:
        pos = self.file_obj.seek(offset, whence)
        self.sent = self.file_obj.tell()
        return pos

    def read(self, amt: int = -1) -> bytes:
        if self.should_stop and self.should_stop():
            raise RuntimeError("Transfer stopped by user.")
        if self.wait_if_paused:
            self.wait_if_paused()
        chunk = self.file_obj.read(amt)
        if not chunk:
            if self.progress_cb:
                self.progress_cb("upload", self.total_size, self.total_size)
            return b""

        self.sent += len(chunk)
        if self.progress_cb:
            self.progress_cb("upload", self.sent, self.total_size)
        return chunk
