defmodule AirportSim.FlightLogger do
  @moduledoc """
  Centralized event logger writing chronological socket and routing actions to both
  a unified simulation log (`logs/simulation.log`) and per-airport log files (`logs/<IATA>.log`).
  """

  use GenServer, restart: :transient

  alias AirportSim.Directory

  @default_log_dir "logs"

  defstruct [:log_dir, :console, :sim_file, node_files: %{}]

  @doc """
  Starts the FlightLogger GenServer.

  Options:
    - `:log_dir` - directory for log files (defaults to `"logs"`)
    - `:reset` - whether to clear existing `.log` files on startup (defaults to `false`)
    - `:console` - whether to echo logs to stdout (defaults to `true`)
    - `:name` - process registration name (defaults to `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Stops the FlightLogger and closes all open file handles."
  @spec stop(GenServer.server()) :: :ok
  def stop(server \\ __MODULE__) do
    if Process.whereis(server) || is_pid(server) do
      GenServer.stop(server, :normal)
    else
      :ok
    end
  catch
    :exit, _ -> :ok
  end

  @doc "Logs an event synchronously so ordering across concurrent nodes is preserved in log files."
  @spec log(String.t(), String.t(), String.t(), GenServer.server()) :: :ok
  def log(node_tag, action, message, server \\ __MODULE__) do
    if alive?(server) do
      GenServer.call(server, {:log, node_tag, action, message}, 5_000)
    else
      :ok
    end
  catch
    :exit, _ -> :ok
  end

  @doc "Logs a system-level orchestrator event under `[SYSTEM]`."
  @spec log_system(String.t(), String.t(), GenServer.server()) :: :ok
  def log_system(action, message, server \\ __MODULE__) do
    log("SYSTEM", action, message, server)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    log_dir = Keyword.get(opts, :log_dir, @default_log_dir)
    reset? = Keyword.get(opts, :reset, false)
    console? = Keyword.get(opts, :console, true)

    File.mkdir_p!(log_dir)

    if reset? do
      log_dir
      |> File.ls!()
      |> Enum.filter(&String.ends_with?(&1, ".log"))
      |> Enum.each(&File.rm!(Path.join(log_dir, &1)))
    end

    sim_path = Path.join(log_dir, "simulation.log")
    sim_file = File.open!(sim_path, [:append, :utf8])

    {:ok, %__MODULE__{log_dir: log_dir, console: console?, sim_file: sim_file, node_files: %{}}}
  end

  @impl true
  def handle_call({:log, node_tag, action, message}, _from, state) do
    timestamp = DateTime.utc_now() |> DateTime.truncate(:millisecond) |> DateTime.to_iso8601()
    endpoint = format_endpoint(node_tag)
    line = "[#{timestamp}] [#{endpoint}] [#{action}] #{message}\n"

    IO.write(state.sim_file, line)
    {file, updated_files} = fetch_or_open_node_file(state, node_tag)
    IO.write(file, line)

    if state.console do
      IO.write(:stdio, line)
    end

    {:reply, :ok, %{state | node_files: updated_files}}
  end

  @impl true
  def terminate(_reason, state) do
    if state.sim_file, do: File.close(state.sim_file)

    Enum.each(state.node_files, fn {_tag, file} ->
      File.close(file)
    end)

    :ok
  end

  defp format_endpoint("SYSTEM"), do: "SYSTEM"

  defp format_endpoint(iata) do
    case Directory.get_airport(iata) do
      {:ok, info} ->
        role_label = if info.role == :hub, do: "HUB", else: "SPOKE"
        "#{info.iata} (#{role_label} #{info.ip_str}:#{info.port})"

      {:error, :unknown_airport} ->
        iata
    end
  end

  defp fetch_or_open_node_file(state, node_tag) do
    case Map.fetch(state.node_files, node_tag) do
      {:ok, file} ->
        {file, state.node_files}

      :error ->
        path = Path.join(state.log_dir, "#{node_tag}.log")
        file = File.open!(path, [:append, :utf8])
        {file, Map.put(state.node_files, node_tag, file)}
    end
  end

  defp alive?(server) when is_pid(server), do: Process.alive?(server)
  defp alive?(server) when is_atom(server), do: Process.whereis(server) != nil
  defp alive?(_), do: false
end
