import logging
from typing import List, Dict, Any, Callable, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class CommandStatus(Enum):
    """Status of command execution."""
    PENDING = "pending"
    VALIDATED = "validated"
    EXECUTING = "executing"
    SUCCESS = "success"
    FAILED = "failed"


class SolutionCommand:
    """Represents a single command in a solution."""

    def __init__(self, name: str, args: Dict[str, Any], metadata: Dict = None):
        self.name = name
        self.args = args
        self.metadata = metadata or {}
        self.status = CommandStatus.PENDING
        self.result = None
        self.error = None

    def validate(self, available_commands: List[str]) -> bool:
        """Validate that command exists and args are valid."""
        if self.name not in available_commands:
            self.error = f"Command '{self.name}' not found"
            return False
        # TODO: add schema validation for args per command type
        self.status = CommandStatus.VALIDATED
        return True

    def __repr__(self):
        return f"{self.name}({self.args})"


class SolutionExecutor:
    """Execute solution commands on the robot."""

    def __init__(self, robot_interface: Optional[Callable] = None):
        """
        Initialize executor.

        Args:
            robot_interface: Callable that executes commands on robot.
                           Signature: execute(command_name, **args) -> result
        """
        self.robot_interface = robot_interface
        self.available_commands = self._get_available_commands()

    def _get_available_commands(self) -> List[str]:
        """Get list of available commands from robot interface."""
        # Commands available via Bellboy Robot HTTP API
        return [
            # Movement commands
            "slide",
            "slide_forward",
            "slide_backward",
            "twist",
            "twist_left",
            "twist_right",
            # Control commands
            "abort",
            "dock",
            # Utility commands
            "wait",
            "verify_stability",
        ]

    def parse_solution(self, commands: List[str]) -> List[SolutionCommand]:
        """Parse solution commands into executable format.

        Args:
            commands: List of command strings like ["reduce_damping(0.5)", "reset_joint(3)"]

        Returns:
            List of SolutionCommand objects
        """
        parsed = []
        for cmd_str in commands:
            cmd_str = cmd_str.strip()
            try:
                # Parse "command_name(arg1, arg2)" format
                if "(" not in cmd_str:
                    # Simple command with no args
                    cmd_obj = SolutionCommand(cmd_str, {})
                else:
                    name, args_str = cmd_str.split("(", 1)
                    name = name.strip()
                    args_str = args_str.rstrip(")")

                    # Parse arguments (basic parsing)
                    args = {}
                    if args_str:
                        # Try to parse as single value or JSON
                        try:
                            # Try numeric
                            args["value"] = float(args_str)
                        except ValueError:
                            # Try string
                            args["value"] = args_str.strip("'\"")

                    cmd_obj = SolutionCommand(name, args)

                parsed.append(cmd_obj)
                logger.info(f"Parsed: {cmd_obj}")

            except Exception as e:
                logger.error(f"Failed to parse command '{cmd_str}': {str(e)}")
                continue

        return parsed

    def validate_solution(self, commands: List[SolutionCommand]) -> bool:
        """Validate all commands before execution.

        Args:
            commands: List of SolutionCommand objects

        Returns:
            True if all valid, False otherwise
        """
        all_valid = True
        for cmd in commands:
            if not cmd.validate(self.available_commands):
                all_valid = False
                logger.error(f"Validation failed for {cmd}: {cmd.error}")

        return all_valid

    async def execute_solution(
        self, commands: List[str], dry_run: bool = False
    ) -> Dict[str, Any]:
        """Execute solution commands on robot.

        Args:
            commands: List of command strings
            dry_run: If True, validate but don't execute

        Returns:
            Execution result with status and results per command
        """
        # Parse commands
        parsed_commands = self.parse_solution(commands)
        if not parsed_commands:
            return {
                "status": "failed",
                "error": "No valid commands to execute",
                "commands": [],
            }

        # Validate
        if not self.validate_solution(parsed_commands):
            return {
                "status": "validation_failed",
                "error": "One or more commands failed validation",
                "commands": [
                    {
                        "command": str(cmd),
                        "status": cmd.status.value,
                        "error": cmd.error,
                    }
                    for cmd in parsed_commands
                ],
            }

        if dry_run:
            logger.info("DRY RUN: Validation passed, not executing")
            return {
                "status": "dry_run_ok",
                "message": "Validation passed",
                "commands": [{"command": str(cmd), "status": "validated"} for cmd in parsed_commands],
            }

        # Execute
        results = []
        for cmd in parsed_commands:
            try:
                logger.info(f"Executing: {cmd}")
                cmd.status = CommandStatus.EXECUTING

                if self.robot_interface:
                    # Call robot interface with command name and arguments
                    if hasattr(self.robot_interface, 'execute_solution_command'):
                        # RobotInterface async method
                        cmd.result = await self.robot_interface.execute_solution_command(
                            cmd.name, **cmd.args
                        )
                    elif callable(self.robot_interface):
                        # Generic callable
                        cmd.result = await self.robot_interface(cmd.name, **cmd.args)
                    else:
                        raise TypeError("robot_interface must be callable or RobotInterface instance")
                else:
                    # Mock execution for testing
                    logger.warning(f"No robot interface, mocking execution of {cmd}")
                    cmd.result = {"mock": True, "command": cmd.name, "message": f"Mock: {cmd}"}

                cmd.status = CommandStatus.SUCCESS
                results.append({
                    "command": str(cmd),
                    "status": "success",
                    "result": cmd.result,
                })
                logger.info(f"✓ {cmd} succeeded")

            except Exception as e:
                cmd.status = CommandStatus.FAILED
                cmd.error = str(e)
                results.append({
                    "command": str(cmd),
                    "status": "failed",
                    "error": str(e),
                })
                logger.error(f"✗ {cmd} failed: {str(e)}")
                # Continue executing remaining commands
                continue

        return {
            "status": "completed",
            "commands": results,
            "total": len(results),
            "successful": sum(1 for r in results if r["status"] == "success"),
        }

    def get_command_help(self) -> Dict[str, str]:
        """Get help for available commands."""
        return {
            "navigate": "Navigate to POI - navigate(poi_name)",
            "move_arm": "Move arm - move_arm(x, y, z)",
            "move_joint": "Move joint - move_joint(joint_id, angle)",
            "gripper_open": "Open gripper - gripper_open()",
            "gripper_close": "Close gripper - gripper_close()",
            "reduce_damping": "Reduce damping - reduce_damping(factor)",
            "increase_damping": "Increase damping - increase_damping(factor)",
            "reset_joint": "Reset joint - reset_joint(joint_id)",
            "verify_stability": "Verify stability - verify_stability()",
            "recalibrate": "Recalibrate - recalibrate(component)",
            "check_sensor": "Check sensor - check_sensor(sensor_name)",
            "wait": "Wait - wait(seconds)",
            "retry": "Retry last command - retry()",
        }
