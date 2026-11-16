defmodule Mix.Tasks.Simulate do
  @moduledoc """
  Runs the AirportSim 17-airport TCP socket simulation.

  Examples:
    mix simulate
    mix simulate --route "FAI,BRW,ANC,Alice"
    mix simulate --interactive
  """

  use Mix.Task

  @shortdoc "Runs the 17-airport hub-and-spoke socket simulation"

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")
    AirportSim.CLI.run_simulate(args)
  end
end
