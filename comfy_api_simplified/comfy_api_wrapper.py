import json
import sys
import requests
import uuid
import logging
import websockets
import asyncio
from requests.auth import HTTPBasicAuth
from requests.compat import urljoin, urlencode
from comfy_api_simplified.comfy_workflow_wrapper import ComfyWorkflowWrapper

logger = logging.getLogger(__name__)

class ComfyApiWrapper:
    def __init__(self, url: str = "http://127.0.0.1:8188", user: str = "", password: str = ""):
        """
        Initializes the ComfyApiWrapper object.

        Args:
            url (str): The URL of the Comfy API server. Defaults to "http://127.0.0.1:8188".
            user (str): The username for authentication. Defaults to an empty string.
            password (str): The password for authentication. Defaults to an empty string.
        """
        self.url = url
        self.auth = None
        url_without_protocol = url.split("//")[-1]

        if "https" in url:
            ws_protocol = "wss"
        else:
            ws_protocol = "ws"

        if user:
            self.auth = HTTPBasicAuth(user, password)
            ws_url_base = f"{ws_protocol}://{user}:{password}@{url_without_protocol}"
        else:
            ws_url_base = f"{ws_protocol}://{url_without_protocol}"
        self.ws_url = urljoin(ws_url_base, "/ws?clientId={}")

    def queue_prompt(self, prompt: dict, client_id: str = None, extra_data = None) -> dict:
        """
        Queues a prompt for execution.

        Args:
            prompt (dict): The prompt to be executed.
            client_id (str): The client ID for the prompt. Defaults to None.

        Returns:
            dict: The response JSON object.

        Raises:
            Exception: If the request fails with a non-200 status code.
        """
        p = {"prompt": prompt}
        if extra_data:
            p["extra_data"] = extra_data
        logging.info(f"Posting prompt for client {client_id}")
        if client_id:
            p["client_id"] = client_id
        data = json.dumps(p).encode("utf-8")
        logger.info(f"Posting prompt to {self.url}/prompt")
        resp = requests.post(urljoin(self.url, "/prompt"), data=data, auth=self.auth)
        logger.info(f"{resp.status_code}: {resp.reason}")
        if resp.status_code == 200:
            return resp.json()
        else:
            raise Exception(f"Request failed with status code {resp.status_code}: {resp.reason}")

    async def queue_prompt_and_wait(self, prompt: dict, client_id = None, extra_data=None) -> str:
        """
        Queues a prompt for execution and waits for the result.

        Args:
            prompt (dict): The prompt to be executed.

        Returns:
            str: The prompt results.

        Raises:
            Exception: If an execution error occurs.
        """
        
        if client_id is None:
            client_id = str(uuid.uuid4())
            
        logging.info(f"Posting prompt for client {client_id} to {self.url}")
            
        resp = self.queue_prompt(prompt, client_id, extra_data=extra_data)
        
        try:
            prompt_id = resp["prompt_id"]
        except KeyError:
            logging.error(f"Error posting prompt to {self.url}: {resp}")
            sys.exit(1)
        
        logging.info(f"Response: {resp}")
        return await self.wait_for_prompt(prompt_id, client_id, extra_data=extra_data)
        

    async def wait_for_prompt(self, prompt_id: str, client_id = None, extra_data=None) -> str:
        logger.debug(f"Connecting to {self.ws_url.format(client_id).split('@')[-1]}")
        async with websockets.connect(uri=self.ws_url.format(client_id)) as websocket:
            while True:
                # out = ws.recv()
                out = await websocket.recv()
                if isinstance(out, str):
                    message = json.loads(out)
                    if message["type"] == "crystools.monitor":
                        continue
                    logger.debug(message)
                    
                    if message["type"] == "execution_error":
                        data = message["data"]
                        if data["prompt_id"] == prompt_id:
                            logging.info(f"{self.url}: Error computing node {message['data']['node_id']} ({message['data']['node_type']})")
                            logging.info(f"{self.url}: {message['data']['exception_type']}: {message['data']['exception_message']}")
                            raise Exception("Execution error occurred.")
                        
                    if message["type"] == "status":
                        data = message["data"]
                        if data["status"]["exec_info"]["queue_remaining"] == 0:
                            logging.info(f"No more prompts in queue: {prompt_id}")
                            return prompt_id
                        
                    if message["type"] == "executing":
                        data = message["data"]
                        if data["node"] is None:
                            if "prompt_id" in data and data["prompt_id"] == prompt_id:
                                logging.info(f"Done? {message}")
                                return prompt_id

    def queue_and_wait_images(self, prompt: ComfyWorkflowWrapper, output_node_ids: list[str],
                              client_id = None, extra_data = None) -> dict:
        """
        Queues a prompt with a ComfyWorkflowWrapper object and waits for the images to be generated.

        Args:
            prompt (ComfyWorkflowWrapper): The ComfyWorkflowWrapper object representing the prompt.
            output_node_id (str): The title of the output node.

        Returns:
            dict: A dictionary mapping image filenames to their content.

        Raises:
            Exception: If the request fails with a non-200 status code.
        """
        loop = asyncio.get_event_loop()
        prompt_id = loop.run_until_complete(self.queue_prompt_and_wait(prompt, client_id=client_id, extra_data=extra_data))
        logging.info(f"Done with prompt {prompt_id}")
        prompt_result = self.get_history(prompt_id)[prompt_id]
        
        outputs = prompt_result["outputs"]
        keys = list(outputs.keys())
        for node_id in keys:
            if node_id not in output_node_ids:
                del outputs[node_id]                    
               
        return prompt_result["outputs"]
    
    
    async def queue_and_wait_images_async(self, prompt: ComfyWorkflowWrapper, output_node_ids: list[str],
                              client_id = None, extra_data = None) -> dict:
        """
        Queues a prompt with a ComfyWorkflowWrapper object and waits for the images to be generated.

        Args:
            prompt (ComfyWorkflowWrapper): The ComfyWorkflowWrapper object representing the prompt.
            output_node_id (str): The title of the output node.

        Returns:
            dict: A dictionary mapping image filenames to their content.

        Raises:
            Exception: If the request fails with a non-200 status code.
        """
        prompt_id = await self.queue_prompt_and_wait(prompt, client_id=client_id, extra_data=extra_data)
        logging.info(f"Done with prompt {prompt_id}")
        prompt_result = self.get_history(prompt_id)[prompt_id]
        
        outputs = prompt_result["outputs"]
        keys = list(outputs.keys())
        for node_id in keys:
            if node_id not in output_node_ids:
                del outputs[node_id]                    
               
        return prompt_result["outputs"]
    

    def get_history(self, prompt_id: str) -> dict:
        """
        Retrieves the execution history for a prompt.

        Args:
            prompt_id (str): The ID of the prompt.

        Returns:
            dict: The response JSON object.

        Raises:
            Exception: If the request fails with a non-200 status code.
        """
        url = urljoin(self.url, f"/history/{prompt_id}")
        logger.debug(f"Getting history from {url}")
        resp = requests.get(url, auth=self.auth)
        if resp.status_code == 200:
            return resp.json()
        else:
            raise Exception(f"Request failed with status code {resp.status_code}: {resp.reason}")

    def get_image(self, filename: str, subfolder: str, folder_type: str) -> bytes:
        """
        Retrieves an image from the Comfy API server.

        Args:
            filename (str): The filename of the image.
            subfolder (str): The subfolder of the image.
            folder_type (str): The type of the folder.

        Returns:
            bytes: The content of the image.

        Raises:
            Exception: If the request fails with a non-200 status code.
        """
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url = urljoin(self.url, f"/view?{urlencode(params)}")
        logger.info(f"Getting image from {url}")
        resp = requests.get(url, auth=self.auth)
        logger.info(f"{resp.status_code}: {resp.reason}")
        if resp.status_code == 200:
            return resp.content
        else:
            raise Exception(f"Request failed with status code {resp.status_code}: {resp.reason}")

    def upload_image(self, filename: str, subfolder: str = "default_upload_folder") -> dict:
        """
        Uploads an image to the Comfy API server.

        Args:
            filename (str): The filename of the image.
            subfolder (str): The subfolder to upload the image to. Defaults to "default_upload_folder".

        Returns:
            dict: The response JSON object.

        Raises:
            Exception: If the request fails with a non-200 status code.
        """
        url = urljoin(self.url, "/upload/image")
        serv_file = filename.split("/")[-1]
        data = {"subfolder": subfolder}
        files = {"image": (serv_file, open(filename, "rb"))}
        logger.info(f"Posting {filename} to {url} with data {data}")
        resp = requests.post(url, files=files, data=data, auth=self.auth)
        logger.info(f"{resp.status_code}: {resp.reason}, {resp.text}")
        if resp.status_code == 200:
            return resp.json()
        else:
            raise Exception(f"Request failed with status code {resp.status_code}: {resp.reason}")

    def validate_prompt(self, prompt: dict) -> dict:
        """
        Validates a prompt by sending it to the /validate_prompt endpoint.

        Args:
            prompt (dict): The prompt to be validated.

        Returns:
            dict: The validation result containing 'valid', 'error_msg', and 'node_errors' keys.

        Raises:
            Exception: If the request fails with a non-200 status code.
        """
        url = urljoin(self.url, "/validate_prompt")
        data = json.dumps({"prompt": prompt}).encode("utf-8")
        logger.debug(f"Validating prompt at {url}")
        resp = requests.post(url, data=data, auth=self.auth)
        logger.debug(f"{resp.status_code}: {resp.reason}")
        if resp.status_code == 200:
            return resp.json()
        else:
            raise Exception(f"Request failed with status code {resp.status_code}: {resp.reason}")
