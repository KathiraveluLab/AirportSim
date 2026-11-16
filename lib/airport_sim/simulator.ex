defmodule AirportSim.Simulator do
  @moduledoc """
  OTP Supervisor and simulation orchestrator for the 17-airport Alaska Airlines
  hub-and-spoke TCP socket network. Executes direct, layover, and policy-enforcement
  scenarios, collects per-node routing metrics, and performs clean socket shutdown.
  """

  use Supervisor

  alias AirportSim.{AirportNode, Directory, FlightLogger, Message}

  @type scenario :: %{
          id: String.t(),
          group: String.t(),
          from: String.t(),
          to: String.t(),
          layover: String.t() | nil,
          airline: String.t(),
          payload: String.t(),
          expect_valid?: boolean()
        }

  @doc "Starts the supervision tree containing NodeRegistry, FlightLogger, and all 17 AirportNode peers."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, AirportSim.NetworkSupervisor)
    opts = Keyword.put_new(opts, :subscriber, self())

    case Supervisor.start_link(__MODULE__, opts, name: name) do
      {:ok, pid} ->
        FlightLogger.log_system(
          "SIM_START",
          "Booting Alaska Airlines hub-and-spoke simulation across #{length(Directory.all_codes())} airports (2 hubs: ANC, SEA; 15 spokes)."
        )

        FlightLogger.log_system(
          "NETWORK_READY",
          "All #{length(Directory.all_codes())} airport TCP server sockets initialized and listening."
        )

        {:ok, pid}

      error ->
        error
    end
  end

  @impl true
  def init(opts) do
    log_dir = Keyword.get(opts, :log_dir, "logs")
    reset_logs = Keyword.get(opts, :reset_logs, true)
    console = Keyword.get(opts, :console, true)
    subscriber = Keyword.get(opts, :subscriber, self())

    node_children =
      Enum.map(Directory.all_airports(), fn info ->
        Supervisor.child_spec(
          {AirportNode,
           [
             iata: info.iata,
             name: AirportNode.via_tuple(info.iata),
             subscribers: [subscriber]
           ]},
          id: {:airport_node, info.iata}
        )
      end)

    children =
      [
        {Registry, keys: :unique, name: AirportSim.NodeRegistry},
        {FlightLogger, [log_dir: log_dir, reset: reset_logs, console: console]}
      ] ++ node_children

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Starts the supervised 17-airport network and returns `{:ok, codes}`.
  """
  @spec start_network(keyword()) :: {:ok, [String.t()]} | {:error, term()}
  def start_network(opts \\ []) do
    case start_link(opts) do
      {:ok, _pid} -> {:ok, Directory.all_codes()}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Gracefully stops all 17 airport nodes, closes the FlightLogger, and stops the supervisor."
  @spec stop_network([String.t()]) :: :ok
  def stop_network(codes \\ Directory.all_codes()) do
    if Process.whereis(FlightLogger) do
      FlightLogger.log_system(
        "SIM_SHUTDOWN",
        "Initiating graceful shutdown across #{length(codes)} airport nodes."
      )
    end

    stop_nodes(codes)

    if Process.whereis(FlightLogger) do
      FlightLogger.log_system("SIM_COMPLETE", "All airport sockets closed cleanly.")
    end

    if pid = Process.whereis(AirportSim.NetworkSupervisor) do
      Supervisor.stop(pid, :normal, 10_000)
    end

    :ok
  catch
    :exit, _ -> :ok
  end

  @doc "Runs the complete default simulation suite and prints the node statistics summary."
  @spec run_all(keyword()) :: {:ok, map()} | {:error, term()}
  def run_all(opts \\ []) do
    with {:ok, codes} <- start_network(opts) do
      try do
        results = execute_scenarios(default_scenarios())
        snapshots = collect_snapshots(codes)
        print_summary(snapshots, results)
        {:ok, %{results: results, snapshots: snapshots}}
      after
        stop_network(codes)
      end
    end
  end

  @doc "Runs a user-supplied list of scenario maps against a newly booted 17-node network."
  @spec run_scenarios([scenario()], keyword()) :: {:ok, map()} | {:error, term()}
  def run_scenarios(scenarios, opts \\ []) do
    with {:ok, codes} <- start_network(opts) do
      try do
        results = execute_scenarios(scenarios)
        snapshots = collect_snapshots(codes)
        print_summary(snapshots, results)
        {:ok, %{results: results, snapshots: snapshots}}
      after
        stop_network(codes)
      end
    end
  end

  @doc "Executes a list of scenario specifications against an already-running network."
  @spec execute_scenarios([scenario()], timeout()) :: map()
  def execute_scenarios(scenarios, timeout \\ 10_000) do
    {dispatched, rejected} =
      Enum.reduce(scenarios, {[], []}, fn scenario, {ok_acc, rej_acc} ->
        FlightLogger.log_system(
          "SCENARIO",
          "[#{scenario.group}] Dispatching #{scenario.id}: #{format_scenario_route(scenario)} for passenger \"#{scenario.payload}\" (airline: #{scenario.airline})."
        )

        case dispatch_scenario(scenario) do
          {:ok, msg} -> {[msg | ok_acc], rej_acc}
          {:error, reason} -> {ok_acc, [{scenario, reason} | rej_acc]}
        end
      end)

    expected_ids = MapSet.new(dispatched, & &1.id)
    arrived_messages = wait_for_arrivals(expected_ids, %{}, timeout)

    %{
      total: length(scenarios),
      dispatched: Enum.reverse(dispatched),
      arrived: arrived_messages,
      rejected: Enum.reverse(rejected)
    }
  end

  @doc "Collects state snapshots for the given airport codes."
  @spec collect_snapshots([String.t()]) :: [map()]
  def collect_snapshots(codes \\ Directory.all_codes()) do
    Enum.map(codes, fn code ->
      code |> AirportNode.via_tuple() |> AirportNode.get_state()
    end)
  end

  @doc "Returns the complete 32-scenario demonstration suite covering all 17 airports and routing policies."
  @spec default_scenarios() :: [scenario()]
  def default_scenarios do
    [
      # Group 1: Direct Spoke -> Hub
      sc(
        "MSG-101",
        "Direct Spoke->Hub",
        "BRW",
        "ANC",
        nil,
        "Alaska Airlines",
        "Alice Kugzruk",
        true
      ),
      sc(
        "MSG-102",
        "Direct Spoke->Hub",
        "FAI",
        "ANC",
        nil,
        "Alaska Airlines",
        "Marcus Chen",
        true
      ),
      sc(
        "MSG-103",
        "Direct Spoke->Hub",
        "OTZ",
        "ANC",
        nil,
        "Alaska Airlines",
        "Nora Savok",
        true
      ),
      sc(
        "MSG-104",
        "Direct Spoke->Hub",
        "ADK",
        "ANC",
        nil,
        "Alaska Airlines",
        "Grace Aleut",
        true
      ),
      sc(
        "MSG-105",
        "Direct Spoke->Hub",
        "JNU",
        "SEA",
        nil,
        "Alaska Airlines",
        "Liam Mercer",
        true
      ),
      sc(
        "MSG-106",
        "Direct Spoke->Hub",
        "KTN",
        "SEA",
        nil,
        "Alaska Airlines",
        "Hannah Cho",
        true
      ),
      sc(
        "MSG-107",
        "Direct Spoke->Hub",
        "SIT",
        "SEA",
        nil,
        "Alaska Airlines",
        "Owen Alsek",
        true
      ),

      # Group 2: Direct Hub -> Spoke
      sc(
        "MSG-201",
        "Direct Hub->Spoke",
        "SEA",
        "FAI",
        nil,
        "Alaska Airlines",
        "Elena Novak",
        true
      ),
      sc(
        "MSG-202",
        "Direct Hub->Spoke",
        "ANC",
        "BRW",
        nil,
        "Alaska Airlines",
        "Toby Ahmaogak",
        true
      ),
      sc(
        "MSG-203",
        "Direct Hub->Spoke",
        "ANC",
        "DUT",
        nil,
        "Alaska Airlines",
        "Victor Tan",
        true
      ),
      sc(
        "MSG-204",
        "Direct Hub->Spoke",
        "SEA",
        "KTN",
        nil,
        "Alaska Airlines",
        "Claire Dawson",
        true
      ),

      # Group 3: Direct Hub <-> Hub
      sc(
        "MSG-301",
        "Direct Hub<->Hub",
        "ANC",
        "SEA",
        nil,
        "Alaska Airlines",
        "Daniel Brooks",
        true
      ),
      sc("MSG-302", "Direct Hub<->Hub", "SEA", "ANC", nil, "Alaska Airlines", "Maya Lin", true),

      # Group 4: Spoke -> ANC -> Spoke (1 Layover at Anchorage Hub)
      sc(
        "MSG-401",
        "Layover via ANC",
        "FAI",
        "BRW",
        "ANC",
        "Alaska Airlines",
        "Samuel Panik",
        true
      ),
      sc(
        "MSG-402",
        "Layover via ANC",
        "OTZ",
        "NME",
        "ANC",
        "Alaska Airlines",
        "Rosa Ivanoff",
        true
      ),
      sc(
        "MSG-403",
        "Layover via ANC",
        "BET",
        "AKN",
        "ANC",
        "Alaska Airlines",
        "Kevin Wassillie",
        true
      ),
      sc(
        "MSG-404",
        "Layover via ANC",
        "DLG",
        "SCC",
        "ANC",
        "Alaska Airlines",
        "Laura Berg",
        true
      ),
      sc(
        "MSG-405",
        "Layover via ANC",
        "CDV",
        "ADQ",
        "ANC",
        "Alaska Airlines",
        "Peter Eklund",
        true
      ),
      sc(
        "MSG-406",
        "Layover via ANC",
        "DUT",
        "ADK",
        "ANC",
        "Alaska Airlines",
        "Sophia Krukoff",
        true
      ),
      sc(
        "MSG-407",
        "Layover via ANC",
        "ADQ",
        "CDV",
        "ANC",
        "Alaska Airlines",
        "Evan Lindholm",
        true
      ),
      sc(
        "MSG-408",
        "Layover via ANC",
        "SCC",
        "OTZ",
        "ANC",
        "Alaska Airlines",
        "Nina Tagarook",
        true
      ),
      sc(
        "MSG-409",
        "Layover via ANC",
        "NME",
        "BET",
        "ANC",
        "Alaska Airlines",
        "Caleb Okpeaha",
        true
      ),
      sc(
        "MSG-410",
        "Layover via ANC",
        "AKN",
        "DLG",
        "ANC",
        "Alaska Airlines",
        "Julia Trefon",
        true
      ),

      # Group 5: Spoke -> SEA -> Spoke (1 Layover at Seattle Hub)
      sc(
        "MSG-501",
        "Layover via SEA",
        "JNU",
        "SIT",
        "SEA",
        "Alaska Airlines",
        "Isaac Morales",
        true
      ),
      sc(
        "MSG-502",
        "Layover via SEA",
        "KTN",
        "JNU",
        "SEA",
        "Alaska Airlines",
        "Rachel Bennett",
        true
      ),
      sc(
        "MSG-503",
        "Layover via SEA",
        "FAI",
        "KTN",
        "SEA",
        "Alaska Airlines",
        "Thomas Alvarez",
        true
      ),

      # Group 6: Cross-Hub Layover
      sc(
        "MSG-601",
        "Cross-Hub Layover",
        "SEA",
        "BRW",
        "ANC",
        "Alaska Airlines",
        "Olivia Hayes",
        true
      ),
      sc(
        "MSG-602",
        "Cross-Hub Layover",
        "OTZ",
        "SEA",
        "ANC",
        "Alaska Airlines",
        "Benjamin Park",
        true
      ),

      # Group 7: Policy Enforcement & Rejections
      sc(
        "MSG-901",
        "Policy Rejection",
        "BRW",
        "OTZ",
        "FAI",
        "Alaska Airlines",
        "Non-Hub Transit Test",
        false
      ),
      sc(
        "MSG-902",
        "Policy Rejection",
        "BRW",
        "OTZ",
        nil,
        "Alaska Airlines",
        "Direct Spoke Test",
        false
      ),
      sc(
        "MSG-903",
        "Policy Rejection",
        "ANC",
        "LAX",
        nil,
        "Alaska Airlines",
        "Out-of-Scope Airport Test",
        false
      ),
      sc(
        "MSG-904",
        "Policy Rejection",
        "ANC",
        "SEA",
        nil,
        "Delta Air Lines",
        "Non-Alaska Carrier Test",
        false
      )
    ]
  end

  defp sc(id, group, from, to, layover, airline, payload, expect_valid?) do
    %{
      id: id,
      group: group,
      from: from,
      to: to,
      layover: layover,
      airline: airline,
      payload: payload,
      expect_valid?: expect_valid?
    }
  end

  defp dispatch_scenario(scenario) do
    case Directory.get_airport(scenario.from) do
      {:ok, _info} ->
        scenario.from
        |> AirportNode.via_tuple()
        |> AirportNode.send_passenger(%{
          id: scenario.id,
          to: scenario.to,
          layover: scenario.layover,
          airline: scenario.airline,
          payload: scenario.payload
        })

      {:error, :unknown_airport} ->
        detail =
          "Origin airport #{inspect(scenario.from)} is outside the supported Alaska and Seattle network."

        FlightLogger.log_system("POLICY_REJECT", detail)
        {:error, {:unknown_airport, detail}}
    end
  end

  defp wait_for_arrivals(pending_ids, arrived_map, timeout) do
    if MapSet.size(pending_ids) == 0 do
      arrived_map
    else
      receive do
        {:airport_event, _iata, :arrived, %Message{id: id} = msg} ->
          if MapSet.member?(pending_ids, id) do
            wait_for_arrivals(
              MapSet.delete(pending_ids, id),
              Map.put(arrived_map, id, msg),
              timeout
            )
          else
            wait_for_arrivals(pending_ids, arrived_map, timeout)
          end

        {:airport_event, _iata, _other, _data} ->
          wait_for_arrivals(pending_ids, arrived_map, timeout)
      after
        timeout ->
          FlightLogger.log_system(
            "TIMEOUT_WARN",
            "Timed out waiting for arrivals: #{inspect(MapSet.to_list(pending_ids))}."
          )

          arrived_map
      end
    end
  end

  defp format_scenario_route(%{from: f, to: t, layover: nil}), do: "#{f} -> #{t}"
  defp format_scenario_route(%{from: f, to: t, layover: l}), do: "#{f} -> #{l} -> #{t}"

  defp print_summary(snapshots, results) do
    header =
      "\n=== AirportSim Network Summary (Delivered: #{map_size(results.arrived)}/#{length(results.dispatched)}, Rejected: #{length(results.rejected)}) ===\n"

    columns =
      String.pad_trailing("IATA", 6) <>
        String.pad_trailing("Role", 7) <>
        String.pad_trailing("Socket Address", 17) <>
        String.pad_leading("Sent", 6) <>
        String.pad_leading("Fwd", 6) <>
        String.pad_leading("Arr", 6) <>
        String.pad_leading("Rej", 6)

    divider = String.duplicate("-", String.length(columns))

    rows =
      Enum.map(snapshots, fn snap ->
        role_str = if snap.role == :hub, do: "HUB", else: "SPOKE"

        String.pad_trailing(snap.iata, 6) <>
          String.pad_trailing(role_str, 7) <>
          String.pad_trailing(snap.address, 17) <>
          String.pad_leading(Integer.to_string(snap.stats.sent), 6) <>
          String.pad_leading(Integer.to_string(snap.stats.forwarded), 6) <>
          String.pad_leading(Integer.to_string(snap.stats.arrived), 6) <>
          String.pad_leading(Integer.to_string(snap.stats.rejected), 6)
      end)

    summary_text = Enum.join([header, columns, divider] ++ rows ++ [divider, ""], "\n")
    IO.puts(summary_text)

    FlightLogger.log_system(
      "SUMMARY",
      "Simulation complete: #{map_size(results.arrived)} delivered over TCP, #{length(results.rejected)} rejected by policy."
    )
  end

  defp stop_nodes(codes) do
    if Process.whereis(AirportSim.NodeRegistry) do
      Enum.each(codes, fn code ->
        case Registry.lookup(AirportSim.NodeRegistry, code) do
          [{pid, _}] -> AirportNode.stop(pid)
          [] -> :ok
        end
      end)
    end
  end
end
