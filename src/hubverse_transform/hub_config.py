import json
import os

from cloudpathlib import AnyPath


class HubConfig:
    """
    Represents the configuration of a Hubverse hub.

    Attributes
    ----------
    """

    def __init__(
        self,
        hub_path: os.PathLike,
        config_dir: str = "hub-config",
        admin_file: str = "admin.json",
        tasks_file: str = "tasks.json",
    ):
        """
        Parameters
        ----------
        hub_path : os.PathLike
            The location of a Hubverse hub
            (e.g., S3 bucket name, local filepath).
        config_dir : str, default="hub-config"
            Directory containing the hub's configuration files
        admin_file : str, default="admin.json"
            Filename of the hub's admin configuration file.
        tasks_file : str, default="tasks.json"
            Filename of the hub's tasks configuration file.
        """

        self.hub_config_path = AnyPath(hub_path) / config_dir
        self.admin = self._get_admin_config(admin_file)
        self.tasks = self._get_tasks_config(tasks_file)
        self.hub_name = self.admin.get("name", "unknown hub name")

    def __repr__(self):
        return f"HubConfig('{str(self.hub_config_path)}')"

    def __str__(self):
        return f"Hubverse config information for {self.hub_name}."

    def _get_admin_config(self, admin_file: str):
        """Read a Hubverse hub's admin configuration file."""
        admin_path = self.hub_config_path / admin_file

        if not admin_path.exists():
            raise FileNotFoundError(f"Hub admin config not found at {str(admin_path)}")

        with admin_path.open() as f:
            return json.loads(f.read())

    def _get_tasks_config(self, tasks_file: str):
        """Read a Hubverse hub's tasks configuration file."""
        tasks_path = self.hub_config_path / tasks_file

        if not tasks_path.exists():
            raise FileNotFoundError(f"Hub tasks config not found at {str(tasks_path)}")

        with tasks_path.open() as f:
            return json.loads(f.read())
