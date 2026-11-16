defmodule Mix.Tasks.Airport.Node do
  @moduledoc """
  Starts a standalone peer AirportNode TCP server in the current terminal.

  Example:
    mix airport.node ANC
  """

  use Mix.Task

  @shortdoc "Starts a standalone airport peer node on its configured TCP port"

  @impl Mix.Task
  def run([iata | _]) do
    Mix.Task.run("app.start")
    AirportSim.CLI.run_peer_node(iata)
  end

  def run([]) do
    Mix.raise("Usage: mix airport.node <IATA> (e.g., mix airport.node ANC)")
  end
end

defmodule Mix.Tasks.Airport.Send do
  @moduledoc """
  Sends a single passenger message over TCP to a running AirportNode server.

  Example:
    mix airport.send --from FAI --to BRW --layover ANC --passenger "Jane Doe"
  """

  use Mix.Task

  @shortdoc "Sends a passenger message over TCP to a running airport network"

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {opts, _rest, invalid} =
      OptionParser.parse(args,
        strict: [
          from: :string,
          to: :string,
          layover: :string,
          airline: :string,
          passenger: :string
        ],
        aliases: [f: :from, t: :to, l: :layover, a: :airline, p: :passenger]
      )

    if invalid != [] do
      Mix.raise("Invalid options: #{inspect(invalid)}")
    end

    from = Keyword.get(opts, :from) || Mix.raise("Missing required option --from")
    to = Keyword.get(opts, :to) || Mix.raise("Missing required option --to")
    passenger = Keyword.get(opts, :passenger) || Mix.raise("Missing required option --passenger")
    layover = Keyword.get(opts, :layover)
    airline = Keyword.get(opts, :airline, AirportSim.Directory.default_airline())

    {:ok, _} = AirportSim.FlightLogger.start_link(reset: false, console: true)

    try do
      case AirportSim.AirportNode.transmit_standalone(%{
             from: from,
             to: to,
             layover: layover,
             airline: airline,
             payload: passenger
           }) do
        {:ok, msg} ->
          IO.puts("Dispatched #{msg.id}: #{AirportSim.Message.format_route(msg)}")

        {:error, {reason, detail}} ->
          Mix.raise("Route rejected (#{reason}): #{detail}")
      end
    after
      AirportSim.FlightLogger.stop()
    end
  end
end
