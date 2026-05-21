"""Detached sidecar supervisor for rspm."""

from dataclasses import dataclass
import os
from pathlib import Path
import subprocess
import time

from rspm.client import RspmClient


@dataclass(frozen=True)
class RspmSupervisor:
    """Ensure a detached ``rspmd`` sidecar is available for a host program.

    :param host: Daemon TCP host.
    :type host: str
    :param port: Daemon TCP port.
    :type port: int
    :param rspm_bin: Path to the ``rspm`` executable.
    :type rspm_bin: str | Path | None
    :param log_dir: Directory for daemon stdout/stderr and task logs.
    :type log_dir: str | Path
    :param state_dir: Directory for pid, applied config, and event state.
    :type state_dir: str | Path
    :param socket_path: Unix socket path used by daemon run arguments.
    :type socket_path: str | Path
    :param token: Optional local auth token.
    :type token: str | None
    :param startup_timeout: Seconds to wait for daemon readiness.
    :type startup_timeout: float
    :param ownership: Sidecar ownership policy. Currently only ``detached`` is supported.
    :type ownership: str
    """

    host: str = "127.0.0.1"
    port: int = 27691
    rspm_bin: str | Path | None = None
    log_dir: str | Path = ".rspm/logs"
    state_dir: str | Path = ".rspm/state"
    socket_path: str | Path = ".rspm/run/rspmd.sock"
    token: str | None = None
    startup_timeout: float = 10.0
    ownership: str = "detached"

    def __post_init__(self) -> None:
        if self.ownership != "detached":
            raise ValueError(f"unsupported ownership [{self.ownership}]")

    @property
    def endpoint(self) -> str:
        """Return the sidecar TCP endpoint."""

        return f"tcp://{self.host}:{self.port}"

    def client(self) -> RspmClient:
        """Create a TCP client for the configured sidecar endpoint."""

        client = RspmClient.connect_tcp(self.host, self.port)
        if self.token is not None:
            client.with_token(self.token)
        return client

    def daemon_command(self, config_path: str | Path) -> list[str]:
        """Build the detached ``rspm daemon run`` command."""

        command = [
            str(self.rspm_bin or os.environ.get("RSPM_BIN", "rspm")),
            "daemon",
            "run",
            str(config_path),
            f"{self.host}:{self.port}",
            str(self.log_dir),
            str(self.state_dir),
            str(self.socket_path),
        ]
        if self.token is not None:
            command.extend(["--token", self.token])
        return command

    def ensure_daemon(self, config_path: str | Path) -> RspmClient:
        """Return a ready client, spawning a detached sidecar if needed."""

        if self._is_ready():
            return self.client()

        self._ensure_config_source(config_path)
        self._spawn_daemon(config_path)
        return self._wait_ready()

    def _is_ready(self) -> bool:
        try:
            self.client().list_tasks()
        except OSError:
            return False
        except RuntimeError:
            return False
        return True

    def _ensure_config_source(self, config_path: str | Path) -> None:
        config = Path(config_path)
        applied_config = Path(self.state_dir) / "applied.toml"
        if config.exists() or applied_config.exists():
            return
        raise FileNotFoundError(
            f"missing config [{config}] and no applied config [{applied_config}]"
        )

    def _spawn_daemon(self, config_path: str | Path) -> None:
        log_dir = Path(self.log_dir)
        state_dir = Path(self.state_dir)
        socket_path = Path(self.socket_path)
        log_dir.mkdir(parents=True, exist_ok=True)
        state_dir.mkdir(parents=True, exist_ok=True)
        if socket_path.parent != Path(""):
            socket_path.parent.mkdir(parents=True, exist_ok=True)

        kwargs: dict[str, object] = {}
        if os.name == "nt":
            kwargs["creationflags"] = (
                subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
            )
        else:
            kwargs["start_new_session"] = True

        with (log_dir / "rspmd.stdout.log").open("ab") as stdout, (
            log_dir / "rspmd.stderr.log"
        ).open("ab") as stderr:
            process = subprocess.Popen(
                self.daemon_command(config_path),
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                **kwargs,
            )
        (state_dir / "rspmd.pid").write_text(str(process.pid))

    def _wait_ready(self) -> RspmClient:
        deadline = time.monotonic() + self.startup_timeout
        while True:
            if self._is_ready():
                return self.client()
            if time.monotonic() >= deadline:
                raise TimeoutError(f"rspmd did not become ready at [{self.host}:{self.port}]")
            time.sleep(0.1)
