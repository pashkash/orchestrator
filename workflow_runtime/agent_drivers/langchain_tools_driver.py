"""LangChain tool-calling runtime driver."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import BaseTool, tool
from langchain_openai import ChatOpenAI
from lmnr import Laminar, observe

from workflow_runtime.agent_drivers.base_driver import BaseDriver, DriverRequest, DriverResult
from workflow_runtime.agent_drivers.yaml_contract import (
    coerce_payload,
    extract_text_content,
    missing_required_payload_keys,
    normalize_payload_shape,
    normalize_status,
    required_payload_keys,
    resolve_provider_model_name,
    status_for_parse_failure,
)
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.phase_config_loader import (
    normalize_runtime_alias_string,
    normalize_runtime_alias_string_for_docs_roots,
)
from workflow_runtime.integrations.runtime_logging import get_logger

logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.langchain_tools_driver.langchain_tools_driver:v1
# type: CLASS
# use_case: Выполняет runtime step через direct tool-calling LLM loop без OpenHands.
# feature:
#   - Нужен для collector/test/runtime-validation шагов, где tools нужны, но полный OH loop даёт лишний overhead
#   - Task card 2026-04-05_1900__oh-laminar-otel-gui, T43
# pre:
#   -
# post:
#   -
# invariant:
#   - доступ к файловой системе ограничен runtime roots из task_context
# modifies (internal):
#   -
# emits (external):
#   - external.llm_provider
#   - file.task_history
#   - file.workspace
# errors:
#   - RuntimeError: tool agent execution failed
# depends:
#   - ChatOpenAI
# sft: execute one task unit step through a LangChain tool-calling agent with filesystem and shell tools
# idempotent: false
# logs: query: LangChainToolsDriver trace_id
class LangChainToolsDriver(BaseDriver):
    def __init__(
        self,
        *,
        llm_api_key: str | None,
        llm_base_url: str,
        timeout_seconds: int,
        max_iterations: int,
        shell_timeout_seconds: int,
        max_output_chars: int,
    ) -> None:
        self._llm_api_key = llm_api_key
        self._llm_base_url = llm_base_url
        self._timeout_seconds = timeout_seconds
        self._max_iterations = max_iterations
        self._shell_timeout_seconds = shell_timeout_seconds
        self._max_output_chars = max_output_chars

    def _truncate(self, text: str) -> str:
        normalized = text if len(text) <= self._max_output_chars else text[: self._max_output_chars] + "\n...[truncated]"
        return normalized

    def _allowed_roots(self, request: DriverRequest, *, include_source_roots: bool = False) -> list[Path]:
        task_context = request.task_context
        repo_roots = [
            path
            for path in dict(task_context.get("task_workspace_repos") or {}).values()
            if path
        ]
        source_roots = [
            task_context.get("source_workspace_root"),
            *dict(task_context.get("source_workspace_roots") or {}).values(),
        ]
        candidates = [
            task_context.get("task_worktree_root"),
            task_context.get("task_dir_path"),
            task_context.get("methodology_root_runtime"),
            *repo_roots,
            *(source_roots if include_source_roots else []),
        ]
        roots: list[Path] = []
        for candidate in candidates:
            if not candidate:
                continue
            resolved = Path(str(candidate)).resolve()
            if resolved.exists() and resolved not in roots:
                roots.append(resolved)
        working_dir = Path(request.working_dir).resolve()
        if working_dir.exists() and working_dir not in roots:
            roots.append(working_dir)
        return roots

    def _normalize_runtime_alias_path(self, *, raw_path: str, request: DriverRequest) -> str:
        raw = str(raw_path).strip()
        methodology_root = str(request.task_context.get("methodology_root_runtime") or "").strip()
        if methodology_root:
            return normalize_runtime_alias_string_for_docs_roots(
                raw,
                docs_roots=[Path(methodology_root)],
            )
        return normalize_runtime_alias_string(raw)

    def _resolve_path(
        self,
        *,
        raw_path: str,
        request: DriverRequest,
        allow_missing: bool = False,
        include_source_roots: bool = False,
    ) -> Path:
        normalized_raw_path = self._normalize_runtime_alias_path(raw_path=raw_path, request=request)
        path = Path(normalized_raw_path)
        candidate = path if path.is_absolute() else (Path(request.working_dir) / path)
        resolved = candidate.resolve(strict=False)
        allowed_roots = self._allowed_roots(request, include_source_roots=include_source_roots)
        for root in allowed_roots:
            try:
                resolved.relative_to(root)
                if not allow_missing and not resolved.exists():
                    raise RuntimeError(f"Path does not exist: {resolved}")
                return resolved
            except ValueError:
                continue
        raise RuntimeError(f"Path is outside allowed runtime roots: {raw_path}")

    @observe(name="tool_read_file")
    def _tool_read_file(self, *, path: str, request: DriverRequest) -> str:
        resolved = self._resolve_path(raw_path=path, request=request, include_source_roots=True)
        return self._truncate(resolved.read_text())

    @observe(name="tool_write_file")
    def _tool_write_file(self, *, path: str, content: str, request: DriverRequest) -> str:
        resolved = self._resolve_path(raw_path=path, request=request, allow_missing=True)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.write_text(content)
        return f"Wrote {len(content)} chars to {resolved}"

    @observe(name="tool_glob")
    def _tool_glob(self, *, pattern: str, target_directory: str, request: DriverRequest) -> str:
        target_root = (
            self._resolve_path(
                raw_path=target_directory,
                request=request,
                include_source_roots=True,
            )
            if target_directory
            else Path(request.working_dir).resolve()
        )
        matches = sorted(str(path) for path in target_root.glob(pattern))
        return self._truncate("\n".join(matches) if matches else "No matches found.")

    @observe(name="tool_rg")
    def _tool_rg(self, *, pattern: str, target_path: str, request: DriverRequest) -> str:
        target = (
            self._resolve_path(
                raw_path=target_path,
                request=request,
                include_source_roots=True,
            )
            if target_path
            else Path(request.working_dir).resolve()
        )
        result = subprocess.run(
            ["rg", "-n", "--color", "never", pattern, str(target)],
            check=False,
            capture_output=True,
            text=True,
            timeout=self._shell_timeout_seconds,
        )
        output = result.stdout if result.returncode in {0, 1} else result.stderr
        if result.returncode == 1 and not output.strip():
            output = "No matches found."
        return self._truncate(output.strip())

    @observe(name="tool_shell")
    def _tool_shell(self, *, command: str, working_directory: str, request: DriverRequest) -> str:
        cwd = (
            self._resolve_path(
                raw_path=working_directory,
                request=request,
                include_source_roots=True,
            )
            if working_directory
            else Path(request.working_dir).resolve()
        )
        result = subprocess.run(
            ["bash", "-lc", command],
            check=False,
            capture_output=True,
            text=True,
            cwd=str(cwd),
            timeout=self._shell_timeout_seconds,
        )
        combined = "\n".join(
            [
                f"exit_code={result.returncode}",
                result.stdout.strip(),
                result.stderr.strip(),
            ]
        ).strip()
        return self._truncate(combined)

    def _build_tools(self, request: DriverRequest) -> list[BaseTool]:
        @tool
        def read_file(path: str) -> str:
            """Read a UTF-8 text file from the allowed runtime roots."""

            return self._tool_read_file(path=path, request=request)

        @tool
        def write_file(path: str, content: str) -> str:
            """Write a UTF-8 text file under the allowed runtime roots."""

            return self._tool_write_file(path=path, content=content, request=request)

        @tool
        def glob_paths(pattern: str, target_directory: str = "") -> str:
            """List filesystem paths matching a glob pattern under the allowed runtime roots."""

            return self._tool_glob(pattern=pattern, target_directory=target_directory, request=request)

        @tool
        def search_contents(pattern: str, target_path: str = "") -> str:
            """Search file contents with ripgrep under the allowed runtime roots."""

            return self._tool_rg(pattern=pattern, target_path=target_path, request=request)

        @tool
        def run_shell(command: str, working_directory: str = "") -> str:
            """Run a shell command in an allowed working directory and return stdout, stderr, and exit code."""

            return self._tool_shell(command=command, working_directory=working_directory, request=request)

        return [read_file, write_file, glob_paths, search_contents, run_shell]

    # SEM_BEGIN orchestrator_v1.langchain_tools_driver.langchain_tools_driver._observe_provider_turn:v1
    # type: METHOD
    # use_case: Оборачивает один provider turn tool-loop/fallback path в отдельный Laminar span и AFL logs.
    # feature:
    #   - Долгие blocking invoke() в LangChain tools runtime должны быть видны как отдельные provider turns
    #   - formatter и repair fallback-и тоже должны иметь такую же observability рамку
    # pre:
    #   - llm is a ready ChatOpenAI or bound-tools runnable with invoke()
    #   - turn_kind identifies tool_loop, formatter_fallback or repair_fallback
    # post:
    #   - returns provider response and records turn-level span attrs/output
    # invariant:
    #   - messages are not mutated inside the helper
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.llm_provider
    # errors:
    #   - propagates provider/runtime exception after recording error outcome
    # depends:
    #   - Laminar
    # sft: observe one langchain tools provider turn with elapsed time and tool-call diagnostics
    # idempotent: false
    # logs: query: LangChainToolsDriver _observe_provider_turn
    def _observe_provider_turn(
        self,
        *,
        llm: Any,
        messages: list[Any],
        trace_id: str,
        phase_id: str,
        role_dir: str,
        sub_role: str,
        turn_kind: str,
        turn_index: int,
    ) -> Any:
        logger.info(
            "[LangChainToolsDriver][_observe_provider_turn][ContextAnchor] trace_id=%s | "
            "Starting provider turn. phase=%s, role_dir=%s, sub_role=%s, turn_kind=%s, turn_index=%d, "
            "message_count=%d, timeout_seconds=%d",
            trace_id,
            phase_id,
            role_dir,
            sub_role,
            turn_kind,
            turn_index,
            len(messages),
            self._timeout_seconds,
        )
        span = Laminar.start_active_span(
            "langchain_tools_provider_turn",
            input={
                "phase": phase_id,
                "role_dir": role_dir,
                "sub_role": sub_role,
                "turn_kind": turn_kind,
                "turn_index": turn_index,
                "message_count": len(messages),
            },
            span_type="LLM",
        )
        Laminar.set_span_attributes(
            {
                "langchain_tools.trace_id": trace_id,
                "langchain_tools.phase": phase_id,
                "langchain_tools.role_dir": role_dir,
                "langchain_tools.sub_role": sub_role,
                "langchain_tools.turn_kind": turn_kind,
                "langchain_tools.turn_index": turn_index,
                "langchain_tools.message_count": len(messages),
                "langchain_tools.timeout_seconds": self._timeout_seconds,
            }
        )
        started_at = time.monotonic()
        try:
            response = llm.invoke(messages)
            elapsed_seconds = round(time.monotonic() - started_at, 3)
            tool_calls = self._extract_tool_calls(response)
            text_content = extract_text_content(getattr(response, "content", ""))
            Laminar.set_span_attributes(
                {
                    "langchain_tools.turn_outcome": "success",
                    "langchain_tools.elapsed_seconds": elapsed_seconds,
                    "langchain_tools.tool_call_count": len(tool_calls),
                    "langchain_tools.response_text_chars": len(text_content),
                }
            )
            Laminar.set_span_output(
                {
                    "status": "success",
                    "turn_kind": turn_kind,
                    "turn_index": turn_index,
                    "elapsed_seconds": elapsed_seconds,
                    "tool_call_count": len(tool_calls),
                    "response_text_chars": len(text_content),
                }
            )
            logger.info(
                "[LangChainToolsDriver][_observe_provider_turn][StepComplete] trace_id=%s | "
                "Provider turn completed. phase=%s, role_dir=%s, sub_role=%s, turn_kind=%s, turn_index=%d, "
                "elapsed_seconds=%.3f, tool_call_count=%d, response_text_chars=%d",
                trace_id,
                phase_id,
                role_dir,
                sub_role,
                turn_kind,
                turn_index,
                elapsed_seconds,
                len(tool_calls),
                len(text_content),
            )
            return response
        except Exception as exc:  # noqa: BLE001
            elapsed_seconds = round(time.monotonic() - started_at, 3)
            Laminar.set_span_attributes(
                {
                    "langchain_tools.turn_outcome": "error",
                    "langchain_tools.elapsed_seconds": elapsed_seconds,
                    "langchain_tools.error": str(exc),
                }
            )
            Laminar.set_span_output(
                {
                    "status": "error",
                    "turn_kind": turn_kind,
                    "turn_index": turn_index,
                    "elapsed_seconds": elapsed_seconds,
                    "error": str(exc),
                }
            )
            logger.error(
                "[LangChainToolsDriver][_observe_provider_turn][ErrorHandled][ERR:EXTERNAL] trace_id=%s | "
                "Provider turn failed. phase=%s, role_dir=%s, sub_role=%s, turn_kind=%s, turn_index=%d, "
                "elapsed_seconds=%.3f, error=%s",
                trace_id,
                phase_id,
                role_dir,
                sub_role,
                turn_kind,
                turn_index,
                elapsed_seconds,
                str(exc),
            )
            raise
        finally:
            span.end()
    # SEM_END orchestrator_v1.langchain_tools_driver.langchain_tools_driver._observe_provider_turn:v1

    # SEM_BEGIN orchestrator_v1.langchain_tools_driver.langchain_tools_driver._extract_tool_calls:v1
    # type: METHOD
    # use_case: Нормализует tool call payload из LangChain/OpenAI response в единый internal shape.
    # feature:
    #   - Tool loop должен одинаково обрабатывать native tool_calls и additional_kwargs fallback
    # pre:
    #   - response is a model response object from ChatOpenAI
    # post:
    #   - returns a list of {id, name, args} tool call dicts
    # invariant:
    #   - malformed tool arguments degrade to empty args dict instead of crashing the whole loop
    # modifies (internal):
    #   -
    # emits (external):
    #   -
    # errors:
    #   - -
    # depends:
    #   - json.loads
    # sft: normalize tool call payloads from a langchain chat response into a consistent internal format
    # idempotent: true
    # logs: query: LangChainToolsDriver _extract_tool_calls
    def _extract_tool_calls(self, response: Any) -> list[dict[str, Any]]:
        raw_calls = getattr(response, "tool_calls", None)
        if isinstance(raw_calls, list):
            return [dict(call) for call in raw_calls]
        additional_kwargs = getattr(response, "additional_kwargs", {})
        raw_calls = additional_kwargs.get("tool_calls")
        if isinstance(raw_calls, list):
            normalized: list[dict[str, Any]] = []
            for call in raw_calls:
                function_payload = call.get("function", {}) if isinstance(call, dict) else {}
                arguments = function_payload.get("arguments", {})
                if isinstance(arguments, str):
                    try:
                        arguments = json.loads(arguments)
                    except json.JSONDecodeError:
                        trace_id = ensure_trace_id()
                        logger.warning(
                            "[LangChainToolsDriver][_extract_tool_calls][ErrorHandled][ERR:DATA_INTEGRITY] trace_id=%s | "
                            "Tool call arguments are not valid JSON. tool_name=%s, raw_arguments=%s",
                            trace_id,
                            function_payload.get("name"),
                            self._truncate(arguments),
                        )
                        arguments = {}
                normalized.append(
                    {
                        "id": call.get("id"),
                        "name": function_payload.get("name"),
                        "args": arguments if isinstance(arguments, dict) else {},
                    }
                )
            return normalized
        return []
    # SEM_END orchestrator_v1.langchain_tools_driver.langchain_tools_driver._extract_tool_calls:v1

    # SEM_BEGIN orchestrator_v1.langchain_tools_driver.langchain_tools_driver._parse_candidate_payload:v1
    # type: METHOD
    # use_case: Парсит candidate YAML text из tool loop в normalized payload/status/missing_keys triple.
    # feature:
    #   - Formatter и repair fallback-и должны использовать одну и ту же parse/normalize logic
    # pre:
    #   - raw_text may be empty or invalid yaml
    # post:
    #   - returns parsed payload, normalized status and missing key list
    # invariant:
    #   - required key validation is always based on the current request.phase_id and request.sub_role
    # modifies (internal):
    #   -
    # emits (external):
    #   -
    # errors:
    #   - -
    # depends:
    #   - coerce_payload
    #   - normalize_payload_shape
    #   - missing_required_payload_keys
    # sft: parse and normalize a candidate yaml payload from the langchain tool loop output
    # idempotent: true
    # logs: -
    def _parse_candidate_payload(self, *, raw_text: str, request: DriverRequest) -> tuple[dict[str, Any] | None, PipelineStatus | None, list[str]]:
        if not raw_text:
            return None, None, required_payload_keys(request.phase_id, request.sub_role)
        parsed_payload = coerce_payload(raw_text)
        if parsed_payload is None:
            return None, None, required_payload_keys(request.phase_id, request.sub_role)
        if "verdict" in parsed_payload and "status" not in parsed_payload:
            parsed_payload["status"] = parsed_payload["verdict"]
        parsed_payload = normalize_payload_shape(
            request.phase_id,
            request.sub_role,
            parsed_payload,
        )
        status = normalize_status(str(parsed_payload.get("status") or "PASS"), request.sub_role)
        missing_keys = missing_required_payload_keys(
            request.phase_id,
            request.sub_role,
            parsed_payload,
            request.task_context,
        )
        return parsed_payload, status, missing_keys
    # SEM_END orchestrator_v1.langchain_tools_driver.langchain_tools_driver._parse_candidate_payload:v1

    # SEM_BEGIN orchestrator_v1.langchain_tools_driver.langchain_tools_driver.run_task:v1
    # type: METHOD
    # use_case: Выполняет runtime step через LangChain tool loop и возвращает DriverResult с parse/repair fallback-ами.
    # feature:
    #   - Tool-capable steps используют один tool loop с prompt caching через SystemMessage/HumanMessage split
    #   - Formatter/repair fallback-и нужны, чтобы довести tool-loop output до YAML contract-а без OpenHands
    # pre:
    #   - request.prompt is not empty
    #   - llm api key is configured
    # post:
    #   - returns DriverResult with normalized payload/status/raw_text
    # invariant:
    #   - tool loop history grows monotonically across iterations within one run_task call
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.llm_provider
    # errors:
    #   - RuntimeError: pre[0] or pre[1] violated
    # depends:
    #   - ChatOpenAI
    #   - _build_tools
    #   - _parse_candidate_payload
    # sft: run a langchain tool-calling step with formatter and repair fallbacks until a valid driver result is produced
    # idempotent: false
    # logs: query: LangChainToolsDriver run_task
    @observe(name="langchain_tool_agent_run_task")
    def run_task(self, request: DriverRequest) -> DriverResult:
        trace_id = ensure_trace_id(request.metadata.get("trace_id"))
        logger.info(
            "[LangChainToolsDriver][run_task][ContextAnchor] trace_id=%s | "
            "Running tool-agent step. phase=%s, role_dir=%s, sub_role=%s, model=%s",
            trace_id,
            request.phase_id,
            request.role_dir,
            request.sub_role,
            request.model,
        )

        if not request.prompt.strip():
            raise RuntimeError("LangChainToolsDriver received an empty prompt")
        if not self._llm_api_key:
            raise RuntimeError("LangChainToolsDriver requires an API key")

        try:
            llm = ChatOpenAI(
                model=resolve_provider_model_name(request.model, self._llm_base_url),
                api_key=self._llm_api_key,
                base_url=self._llm_base_url,
                timeout=self._timeout_seconds,
                temperature=0,
            )
            tools = self._build_tools(request)
            tool_lookup = {tool.name: tool for tool in tools}
            llm_with_tools = llm.bind_tools(tools)
            messages: list[Any] = []
            if request.system_prompt:
                messages.append(SystemMessage(content=request.system_prompt))
                user_content = request.prompt[len(request.system_prompt):].lstrip("\n")
                messages.append(HumanMessage(content=user_content or request.prompt))
            else:
                messages.append(HumanMessage(content=request.prompt))
            last_response: Any = None

            for turn_index in range(1, self._max_iterations + 1):
                response = self._observe_provider_turn(
                    llm=llm_with_tools,
                    messages=messages,
                    trace_id=trace_id,
                    phase_id=str(request.phase_id),
                    role_dir=request.role_dir,
                    sub_role=str(request.sub_role),
                    turn_kind="tool_loop",
                    turn_index=turn_index,
                )
                last_response = response
                messages.append(response)
                tool_calls = self._extract_tool_calls(response)
                if not tool_calls:
                    logger.info(
                        "[LangChainToolsDriver][run_task][DecisionPoint] trace_id=%s | "
                        "Branch: accept_model_response. Reason: provider turn returned no tool calls. "
                        "phase=%s, role_dir=%s, sub_role=%s, turn_index=%d",
                        trace_id,
                        request.phase_id,
                        request.role_dir,
                        request.sub_role,
                        turn_index,
                    )
                    break
                for tool_call in tool_calls:
                    tool_name = str(tool_call.get("name") or "")
                    tool_args = tool_call.get("args") if isinstance(tool_call.get("args"), dict) else {}
                    tool_impl = tool_lookup.get(tool_name)
                    logger.info(
                        "[LangChainToolsDriver][run_task][ExternalCall][BELIEF] trace_id=%s | "
                        "Executing requested tool. phase=%s, role_dir=%s, sub_role=%s, turn_index=%d, tool_name=%s",
                        trace_id,
                        request.phase_id,
                        request.role_dir,
                        request.sub_role,
                        turn_index,
                        tool_name,
                    )
                    if tool_impl is None:
                        tool_result = f"Unknown tool requested: {tool_name}"
                    else:
                        try:
                            tool_result = tool_impl.invoke(tool_args)
                        except Exception as exc:  # noqa: BLE001
                            logger.error(
                                "[LangChainToolsDriver][run_task][ErrorHandled][ERR:EXTERNAL] trace_id=%s | "
                                "Tool execution failed. phase=%s, role_dir=%s, sub_role=%s, tool_name=%s, error=%s",
                                trace_id,
                                request.phase_id,
                                request.role_dir,
                                request.sub_role,
                                tool_name,
                                str(exc),
                            )
                            tool_result = f"Tool execution failed: {exc}"
                    messages.append(
                        ToolMessage(
                            content=self._truncate(str(tool_result)),
                            tool_call_id=str(tool_call.get("id") or tool_name),
                        )
                    )
                    logger.info(
                        "[LangChainToolsDriver][run_task][ExternalCall][GROUND] trace_id=%s | "
                        "Tool execution finished. phase=%s, role_dir=%s, sub_role=%s, turn_index=%d, tool_name=%s, "
                        "result_chars=%d",
                        trace_id,
                        request.phase_id,
                        request.role_dir,
                        request.sub_role,
                        turn_index,
                        tool_name,
                        len(str(tool_result)),
                    )

            last_response_text = extract_text_content(
                last_response.content if last_response is not None else ""
            )
            parsed_payload, status, missing_keys = self._parse_candidate_payload(
                raw_text=last_response_text,
                request=request,
            )
            raw_text = last_response_text

            if parsed_payload is None:
                logger.info(
                    "[LangChainToolsDriver][run_task][DecisionPoint] trace_id=%s | "
                    "Branch: formatter_fallback. Reason: last tool-loop response was not parseable YAML. "
                    "phase=%s, role_dir=%s, sub_role=%s",
                    trace_id,
                    request.phase_id,
                    request.role_dir,
                    request.sub_role,
                )
                formatter_prompt = (
                    "Using the conversation and tool results above, return the final answer now. "
                    "Return exactly one YAML block that satisfies the original output contract. "
                    f"Required top-level keys: {', '.join(required_payload_keys(request.phase_id, request.sub_role))}. "
                    "Do not call tools. Do not add prose before or after the YAML."
                )
                formatter_response = self._observe_provider_turn(
                    llm=llm,
                    messages=[*messages, HumanMessage(content=formatter_prompt)],
                    trace_id=trace_id,
                    phase_id=str(request.phase_id),
                    role_dir=request.role_dir,
                    sub_role=str(request.sub_role),
                    turn_kind="formatter_fallback",
                    turn_index=1,
                )
                raw_text = extract_text_content(
                    formatter_response.content if formatter_response is not None else ""
                )
                parsed_payload, status, missing_keys = self._parse_candidate_payload(
                    raw_text=raw_text,
                    request=request,
                )

            if parsed_payload is None or status is None:
                status = status_for_parse_failure(request.sub_role)
                parsed_payload = {
                    "status": str(status),
                    "warnings": ["Tool agent returned no parseable text output"],
                }
            elif missing_keys:
                logger.info(
                    "[LangChainToolsDriver][run_task][DecisionPoint] trace_id=%s | "
                    "Branch: repair_fallback. Reason: YAML missing required keys=%s. "
                    "phase=%s, role_dir=%s, sub_role=%s",
                    trace_id,
                    ",".join(missing_keys),
                    request.phase_id,
                    request.role_dir,
                    request.sub_role,
                )
                repair_prompt = (
                    "Your previous YAML response was missing required keys: "
                    + ", ".join(missing_keys)
                    + ". Rewrite the answer as exactly one YAML block with all required keys present. "
                    + "Do not call tools. Do not add prose before or after the YAML."
                )
                repair_response = self._observe_provider_turn(
                    llm=llm,
                    messages=[
                        *messages,
                        AIMessage(content=raw_text),
                        HumanMessage(content=repair_prompt),
                    ],
                    trace_id=trace_id,
                    phase_id=str(request.phase_id),
                    role_dir=request.role_dir,
                    sub_role=str(request.sub_role),
                    turn_kind="repair_fallback",
                    turn_index=1,
                )
                repaired_text = extract_text_content(
                    repair_response.content if repair_response is not None else ""
                )
                repaired_payload, repaired_status, repaired_missing = self._parse_candidate_payload(
                    raw_text=repaired_text,
                    request=request,
                )
                if repaired_payload is not None and repaired_status is not None and not repaired_missing:
                    raw_text = repaired_text
                    parsed_payload = repaired_payload
                    status = repaired_status
            else:
                logger.info(
                    "[LangChainToolsDriver][run_task][DecisionPoint] trace_id=%s | "
                    "Branch: accept_last_response. Reason: last tool-loop response already satisfied output contract. "
                    "phase=%s, role_dir=%s, sub_role=%s",
                    trace_id,
                    request.phase_id,
                    request.role_dir,
                    request.sub_role,
                )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "[LangChainToolsDriver][run_task][ErrorHandled][ERR:EXTERNAL] trace_id=%s | "
                "Tool-agent execution failed. phase=%s, role_dir=%s, sub_role=%s, error=%s",
                trace_id,
                request.phase_id,
                request.role_dir,
                request.sub_role,
                str(exc),
            )
            raise RuntimeError(f"LangChain tools driver failed: {exc}") from exc

        logger.info(
            "[LangChainToolsDriver][run_task][StepComplete] trace_id=%s | "
            "Tool-agent step completed. phase=%s, role_dir=%s, sub_role=%s, status=%s",
            trace_id,
            request.phase_id,
            request.role_dir,
            request.sub_role,
            status,
        )
        return DriverResult(
            status=status,
            payload=parsed_payload,
            raw_text=raw_text,
            conversation_id=None,
        )
    # SEM_END orchestrator_v1.langchain_tools_driver.langchain_tools_driver.run_task:v1


# SEM_END orchestrator_v1.langchain_tools_driver.langchain_tools_driver:v1
