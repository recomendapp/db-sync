import logging
from rich.console import Console
from rich.logging import RichHandler

console = Console()

logging.basicConfig(
	level="INFO",
	format="%(message)s",
	datefmt="[%Y-%m-%d %H:%M:%S]",
	handlers=[RichHandler(console=console, show_path=False)],
)

logger = logging.getLogger("rich")