import json
import logging
from typing import Any, List, Union

logger = logging.getLogger(__name__)

class ComfyWorkflowWrapper(dict):
    def __init__(self, workflow_data: Union[str, dict]):
        """
        Initialize the ComfyWorkflowWrapper object.

        Args:
            workflow_data (str): The path to the workflow file or a JSON string representing the workflow.
        """
        if isinstance(workflow_data, dict):
            workflow_dict = workflow_data
        elif isinstance(workflow_data, str):
            if workflow_data.startswith("{"):
                # If the input is a JSON string
                workflow_dict = json.loads(workflow_data)
            else:
                # If the input is a file path
                with open(workflow_data) as f:
                    workflow_str = f.read()
                    workflow_dict = json.loads(workflow_str)
        else:
            raise TypeError("Expected a dictionary")
        super().__init__(workflow_dict)

    def list_nodes(self) -> List[str]:
        """
        Get a list of node titles in the workflow.

        Returns:
            List[Tuple(str, str)]: A list of node IDs and titles.
        """
        return [(id, value["_meta"]["title"]) for id, value in super().items()]

    def set_node_param(self, id: str, param: str, value):
        """
        Set the value of a parameter for a specific node.
        Mind that this method will change parameters for ALL nodes with the same title.

        Args:
            id (str): The I        param (str): The name of the parameter.
            value: The value to set.

        Raises:
            ValueError: If the no    """
        smth_changed = False
        id = str(id)
        if id in self:
            node = self[id]
            orig_value = node["inputs"][param]
            if isinstance(orig_value, (int, float)):
                node["inputs"][param] = value
            elif isinstance(orig_value, bool):
                node["inputs"][param] = bool(value)
            elif isinstance(orig_value, list):
                node["inputs"][param] = json.loads(value)
            else:
                node["inputs"][param] = value
                
            logging.debug(f"Changed node '{id}' parameter '{param}' from '{orig_value}' to '{value}': {node}")
            smth_changed = True
        if not smth_changed:
            raise ValueError(f"Node '{id}' not found.")

    def get_node_param(self, id: str, param: str) -> Any:
        """
        Get the value of a parameter for a specific node.
        Mind that this method will return the value of the first node with this title.

        Args:
            id (str): The id of the node.
            param (str): The name of the parameter.

        Returns:
            The value of the parameter.

        Raises:
            ValueError: If the node is not found.
        """
        id = str(id)
        assert id in self, f"Node '{id}' not found."
        return self[id]["inputs"][param]

    def get_node_id(self, title: str) -> str:
        """
        Get the ID of a specific node.

        Args:
            title (str): The title of the node.

        Returns:
            str: The ID of the node.

        Raises:
            ValueError: If the node is not found.
        """
        for id, node in super().items():
            if node["_meta"]["title"] == title:
                return id
        raise ValueError(f"Node '{title}' not found.")
    
    def prune(workflow, output_nodes, no_cache):
        # Perform a depth-first search to find all required nodes
        required_nodes = set()
        visited_nodes = set()

        def dfs(node_id):
            if node_id in visited_nodes:
                return
            visited_nodes.add(node_id)

            node = workflow[node_id]

            for input_value in node["inputs"].values():
                if isinstance(input_value, list) and len(input_value) == 2:
                    input_node_id = str(input_value[0])
                    dfs(input_node_id)

            required_nodes.add(node_id)

        for output_node in output_nodes:
            dfs(output_node)

        # Remove unnecessary nodes from the workflow
        pruned_workflow = {node_id: node for node_id, node in workflow.items() if node_id in required_nodes}

        return ComfyWorkflowWrapper(pruned_workflow)

    def save_to_file(self, path: str):
        """
        Save the workflow to a file.

        Args:
            path (str): The path to save the workflow file.
        """
        workflow_str = json.dumps(self, indent=4)
        with open(path, "w+") as f:
            f.write(workflow_str)
