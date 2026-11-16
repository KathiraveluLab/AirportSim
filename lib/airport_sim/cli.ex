defmodule AirportSim.CLI do
  @moduledoc """
  Command-line interface and interactive route shell for AirportSim.

  Supports:
    - Default full 17-airport simulation (`mix simulate`)
    - Explicit route flags (`--route "FAI,BRW,ANC,Jane Doe"`)
    - Interactive stdin loop (`--interactive`) detecting termination of input cleanly
    - Escript entry point (`main/1`)
  """

  alias AirportSim.{AirportNode, Directory, FlightLogger, Simulator}

  @doc "Escript entry point."
  @spec main([String.t()]) :: :ok
  def main(args) do
    run_simulate(args)
  end

  @doc """
  Parses CLI flags for simulation execution.

  Options:
    - `--route "FROM,TO,LAYOVER,PASSENGER"` (repeatable; use `NONE` for direct flights)
    - `--interactive` / `-i` - read passenger routes interactively from stdin
    - `--quiet` / `-q` - suppress console log echo (still writes to `logs/`)
    - `--help` / `-h` - print usage instructions
  """
  @spec run_simulate([String.t()]) :: :ok
  def run_simulate(args) do
    {opts, _rest, invalid} =
      OptionParser.parse(args,
        strict: [route: :keep, interactive: :boolean, quiet: :boolean, help: :boolean],
        aliases: [r: :route, i: :interactive, q: :quiet, h: :help]
      )

    cond do
      invalid != [] ->
        IO.puts(:stderr, "Invalid options: #{inspect(invalid)}")
        print_help()

      Keyword.get(opts, :help, false) ->
        print_help()

      Keyword.get(opts, :interactive, false) ->
        run_interactive(console: not Keyword.get(opts, :quiet, false))

      Keyword.has_key?(opts, :route) ->
        routes = Keyword.get_values(opts, :route)
        scenarios = parse_route_specs(routes)
        Simulator.run_scenarios(scenarios, console: not Keyword.get(opts, :quiet, false))
        :ok

      true ->
        Simulator.run_all(console: not Keyword.get(opts, :quiet, false))
        :ok
    end
  end

  @doc "Runs an interactive stdin command loop over all 17 live airport nodes."
  @spec run_interactive(keyword()) :: :ok
  def run_interactive(opts \\ []) do
    with {:ok, codes} <- Simulator.start_network(opts) do
      try do
        IO.puts("\nInteractive AirportSim Shell Ready (17 airports online).")
        IO.puts("Enter route as: FROM,TO,LAYOVER,PASSENGER (use NONE for direct flights)")
        IO.puts("Commands: 'status', 'airports', 'exit', 'quit', or EOF (Ctrl+Z / Ctrl+D)\n")
        interactive_loop(1)
      after
        Simulator.stop_network(codes)
      end
    end

    :ok
  end

  @doc "Parses a list of route strings into scenario maps."
  @spec parse_route_specs([String.t()]) :: [Simulator.scenario()]
  def parse_route_specs(specs) do
    specs
    |> Enum.with_index(1)
    |> Enum.map(fn {spec, idx} ->
      parse_single_route(spec, "CLI-#{String.pad_leading(Integer.to_string(idx), 3, "0")}")
    end)
  end

  @doc "Parses a single route specification string (`FROM,TO,LAYOVER,PASSENGER` or `FROM,TO,PASSENGER`)."
  @spec parse_single_route(String.t(), String.t()) :: Simulator.scenario()
  def parse_single_route(spec, id) do
    parts = spec |> String.split(",") |> Enum.map(&String.trim/1)

    case parts do
      [from, to, passenger] ->
        %{
          id: id,
          group: "CLI Route",
          from: from,
          to: to,
          layover: nil,
          airline: Directory.default_airline(),
          payload: passenger,
          expect_valid?: true
        }

      [from, to, layover, passenger] ->
        %{
          id: id,
          group: "CLI Route",
          from: from,
          to: to,
          layover: Directory.normalize_code(layover),
          airline: Directory.default_airline(),
          payload: passenger,
          expect_valid?: true
        }

      [from, to, layover, airline, passenger] ->
        %{
          id: id,
          group: "CLI Route",
          from: from,
          to: to,
          layover: Directory.normalize_code(layover),
          airline: airline,
          payload: passenger,
          expect_valid?: true
        }

      _ ->
        raise ArgumentError,
              "Invalid --route format #{inspect(spec)}. Expected FROM,TO,LAYOVER,PASSENGER"
    end
  end

  defp interactive_loop(counter) do
    case IO.gets("airport-sim> ") do
      :eof ->
        FlightLogger.log_system(
          "INPUT_EOF",
          "Detected termination of input (EOF) on interactive console."
        )

        :ok

      {:error, reason} ->
        FlightLogger.log_system(
          "INPUT_ERROR",
          "Interactive console input error: #{inspect(reason)}."
        )

        :ok

      line ->
        trimmed = String.trim(line)

        cond do
          trimmed in ["exit", "quit", "*exit"] ->
            FlightLogger.log_system(
              "INPUT_EXIT",
              "User requested termination via '#{trimmed}' command."
            )

            :ok

          trimmed == "" ->
            interactive_loop(counter)

          trimmed == "airports" ->
            Enum.each(Directory.all_airports(), fn info ->
              role_str = if info.role == :hub, do: "HUB  ", else: "SPOKE"
              IO.puts("  #{info.iata} [#{role_str}] #{info.ip_str}:#{info.port} - #{info.name}")
            end)

            interactive_loop(counter)

          trimmed == "status" ->
            Enum.each(Simulator.collect_snapshots(), fn s ->
              IO.puts(
                "  #{s.iata} (#{s.address}): sent=#{s.stats.sent} fwd=#{s.stats.forwarded} arr=#{s.stats.arrived} rej=#{s.stats.rejected}"
              )
            end)

            interactive_loop(counter)

          true ->
            id = "INT-#{String.pad_leading(Integer.to_string(counter), 3, "0")}"

            try do
              scenario = parse_single_route(trimmed, id)
              Simulator.execute_scenarios([scenario])
            rescue
              e in ArgumentError ->
                IO.puts(Exception.message(e))
            end

            interactive_loop(counter + 1)
        end
    end
  end

  @doc "Runs a standalone interactive airport peer node in its own terminal process."
  @spec run_peer_node(String.t(), keyword()) :: :ok
  def run_peer_node(iata, opts \\ []) do
    normalized = Directory.normalize_code(iata)

    case Directory.get_airport(normalized) do
      {:ok, info} ->
        console = Keyword.get(opts, :console, true)
        log_dir = Keyword.get(opts, :log_dir, "logs")
        {:ok, _logger} = FlightLogger.start_link(log_dir: log_dir, reset: false, console: console)
        {:ok, node_pid} = AirportNode.start_link(iata: normalized, subscribers: [self()])

        IO.puts("\nPeer node #{normalized} (#{info.role}) active on #{info.ip_str}:#{info.port}.")

        IO.puts("Commands: 'send TO [LAYOVER] PASSENGER', 'status', 'quit', or EOF.\n")
        peer_command_loop(node_pid, normalized)
        AirportNode.stop(node_pid)
        FlightLogger.stop()
        :ok

      {:error, :unknown_airport} ->
        IO.puts(:stderr, "Unknown airport IATA code: #{inspect(iata)}")
        IO.puts(:stderr, "Valid codes: #{Enum.join(Directory.all_codes(), ", ")}")
        :ok
    end
  end

  defp peer_command_loop(node_pid, iata) do
    case IO.gets("#{iata}> ") do
      :eof ->
        FlightLogger.log(iata, "INPUT_EOF", "Detected EOF on peer terminal #{iata}.")
        :ok

      {:error, _} ->
        :ok

      line ->
        trimmed = String.trim(line)

        cond do
          trimmed in ["exit", "quit"] ->
            FlightLogger.log(iata, "INPUT_EXIT", "User exited peer terminal #{iata}.")
            :ok

          trimmed == "" ->
            peer_command_loop(node_pid, iata)

          trimmed == "status" ->
            snap = AirportNode.get_state(node_pid)
            IO.inspect(snap.stats, label: "#{iata} stats")
            peer_command_loop(node_pid, iata)

          String.starts_with?(trimmed, "send ") ->
            handle_peer_send_command(node_pid, trimmed)
            peer_command_loop(node_pid, iata)

          true ->
            IO.puts("Unrecognized command. Use: send TO [LAYOVER|NONE] PASSENGER | status | quit")
            peer_command_loop(node_pid, iata)
        end
    end
  end

  defp handle_peer_send_command(node_pid, line) do
    parts = String.split(line, ~r/\s+/, parts: 4)

    case parts do
      ["send", to, passenger] ->
        AirportNode.send_passenger(node_pid, %{to: to, layover: nil, payload: passenger})

      ["send", to, layover, passenger] ->
        AirportNode.send_passenger(node_pid, %{
          to: to,
          layover: Directory.normalize_code(layover),
          payload: passenger
        })

      _ ->
        IO.puts("Usage: send TO [LAYOVER|NONE] PASSENGER")
    end
  end

  defp print_help do
    IO.puts("""
    AirportSim - Alaska Airlines Hub-and-Spoke TCP Socket Simulation

    Usage:
      mix simulate
      mix simulate --route "FAI,BRW,ANC,Jane Doe" --route "BRW,ANC,NONE,John Smith"
      mix simulate --interactive
      mix airport.node ANC
      mix airport.send --from FAI --to BRW --layover ANC --passenger "Jane Doe"

    Options:
      -r, --route        Specify one or more routes: "FROM,TO,LAYOVER,PASSENGER"
      -i, --interactive  Launch interactive route entry prompt on stdin
      -q, --quiet        Write log files without echoing log lines to stdout
      -h, --help         Display this help message
    """)
  end
end
