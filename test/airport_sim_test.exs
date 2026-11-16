defmodule AirportSimTest do
  use ExUnit.Case, async: false

  alias AirportSim.{AirportNode, CLI, Directory, Message, Simulator}

  @expected_codes ~w(ANC SEA FAI JNU SIT ADK ADQ OTZ NME BET AKN DLG BRW SCC CDV DUT KTN)

  describe "Directory topology and policy rules" do
    test "contains exactly the 17 required airports with ports > 5000" do
      airports = Directory.all_airports()
      assert length(airports) == 17

      codes = Enum.map(airports, & &1.iata)
      assert Enum.sort(codes) == Enum.sort(@expected_codes)

      ports = Enum.map(airports, & &1.port)
      assert Enum.uniq(ports) == ports
      assert Enum.all?(ports, &(&1 > 5000))
    end

    test "designates only ANC and SEA as hubs and the remaining 15 as spokes" do
      hub_codes = Directory.hubs() |> Enum.map(& &1.iata) |> Enum.sort()
      spoke_codes = Directory.spokes() |> Enum.map(& &1.iata)

      assert hub_codes == ["ANC", "SEA"]
      assert length(spoke_codes) == 15
      refute "FAI" in hub_codes
    end

    test "validates direct, layover, and prohibited routes" do
      assert :ok == Directory.validate_route("BRW", "ANC", nil)
      assert :ok == Directory.validate_route("SEA", "FAI", nil)
      assert :ok == Directory.validate_route("ANC", "SEA", nil)
      assert :ok == Directory.validate_route("FAI", "BRW", "ANC")
      assert :ok == Directory.validate_route("JNU", "KTN", "SEA")

      assert {:error, :direct_spoke_to_spoke_prohibited, _} =
               Directory.validate_route("BRW", "OTZ", nil)

      assert {:error, :invalid_layover_not_a_hub, _} =
               Directory.validate_route("BRW", "OTZ", "FAI")

      assert {:error, :unknown_airport, _} =
               Directory.validate_route("ANC", "LAX", nil)

      assert {:error, :unknown_airport, _} =
               Directory.validate_route("ORD", "SEA", nil)

      assert {:error, :unsupported_airline, _} =
               Directory.validate_route("ANC", "SEA", nil, "Delta Air Lines")

      assert {:error, :same_origin_and_destination, _} =
               Directory.validate_route("ANC", "ANC", nil)

      assert {:ok, "ANC"} == Directory.resolve_route("FAI", "BRW", :auto, "Alaska Airlines")
      assert {:ok, nil} == Directory.resolve_route("BRW", "ANC", :auto, "Alaska Airlines")
    end
  end

  describe "Message serialization" do
    test "encodes and decodes newline-delimited JSON packets" do
      msg =
        Message.new(%{
          id: "MSG-TEST-1",
          from: "fai",
          to: "brw",
          layover: "anc",
          payload: "Alice Kugzruk"
        })

      encoded = Message.encode(msg)
      assert String.ends_with?(encoded, "\n")
      assert {:ok, decoded} = Message.decode(encoded)

      assert decoded.id == "MSG-TEST-1"
      assert decoded.airline == "Alaska Airlines"
      assert decoded.from == "FAI"
      assert decoded.to == "BRW"
      assert decoded.layover == "ANC"
      assert decoded.payload == "Alice Kugzruk"
      assert decoded.hop_count == 1
      assert decoded.route_trace == ["FAI"]
    end

    test "returns error for malformed payloads" do
      assert {:error, _} = Message.decode("not valid json\n")

      assert {:error, {:missing_field, "to"}} =
               Message.decode(~s({"id":"1","from":"ANC","payload":"X"}\n))
    end
  end

  describe "CLI route parser" do
    test "parses 3-part, 4-part, and 5-part route specifications" do
      [s1, s2] =
        CLI.parse_route_specs([
          "BRW,ANC,Jane Doe",
          "FAI,BRW,ANC,John Smith"
        ])

      assert s1.from == "BRW"
      assert s1.to == "ANC"
      assert s1.layover == nil
      assert s1.payload == "Jane Doe"

      assert s2.from == "FAI"
      assert s2.to == "BRW"
      assert s2.layover == "ANC"
      assert s2.payload == "John Smith"
    end
  end

  describe "Live 17-node TCP socket network" do
    setup do
      log_dir =
        Path.join(
          System.tmp_dir!(),
          "airport_sim_test_logs_#{System.unique_integer([:positive])}"
        )

      start_supervised!(
        {Simulator, log_dir: log_dir, reset_logs: true, console: false, subscriber: self()}
      )

      on_exit(fn ->
        File.rm_rf!(log_dir)
      end)

      %{codes: Directory.all_codes(), log_dir: log_dir}
    end

    test "routes direct spoke-to-hub and hub-to-spoke flights over TCP" do
      {:ok, m1} =
        "BRW"
        |> AirportNode.via_tuple()
        |> AirportNode.send_passenger(%{to: "ANC", payload: "Direct Test 1"})

      {:ok, m2} =
        "SEA"
        |> AirportNode.via_tuple()
        |> AirportNode.send_passenger(%{to: "FAI", payload: "Direct Test 2"})

      assert_receive {:airport_event, "ANC", :arrived, %Message{id: id1} = arr1}, 3_000
      assert id1 == m1.id
      assert arr1.hop_count == 1
      assert arr1.route_trace == ["BRW", "ANC"]

      assert_receive {:airport_event, "FAI", :arrived, %Message{id: id2} = arr2}, 3_000
      assert id2 == m2.id
      assert arr2.hop_count == 1
      assert arr2.route_trace == ["SEA", "FAI"]
    end

    test "routes spoke-to-spoke flights through ANC and SEA layover hubs over TCP" do
      {:ok, m1} =
        "FAI"
        |> AirportNode.via_tuple()
        |> AirportNode.send_passenger(%{to: "BRW", layover: "ANC", payload: "Layover ANC Test"})

      assert_receive {:airport_event, "ANC", :forwarded, %Message{id: f_id1}}, 3_000
      assert f_id1 == m1.id
      assert_receive {:airport_event, "BRW", :arrived, %Message{id: a_id1} = arr1}, 3_000
      assert a_id1 == m1.id
      assert arr1.hop_count == 2
      assert arr1.route_trace == ["FAI", "ANC", "BRW"]

      {:ok, m2} =
        "JNU"
        |> AirportNode.via_tuple()
        |> AirportNode.send_passenger(%{to: "KTN", layover: "SEA", payload: "Layover SEA Test"})

      assert_receive {:airport_event, "SEA", :forwarded, %Message{id: f_id2}}, 3_000
      assert f_id2 == m2.id
      assert_receive {:airport_event, "KTN", :arrived, %Message{id: a_id2} = arr2}, 3_000
      assert a_id2 == m2.id
      assert arr2.hop_count == 2
      assert arr2.route_trace == ["JNU", "SEA", "KTN"]
    end

    test "enforces hub-only layover policy on server socket when raw packet targets spoke transit" do
      # Send a raw TCP packet directly to FAI (port 5003) pretending FAI is a layover hub
      {:ok, fai_info} = Directory.get_airport("FAI")

      {:ok, sock} =
        :gen_tcp.connect(
          fai_info.ip,
          fai_info.port,
          [:binary, packet: :line, active: false],
          2_000
        )

      bad_msg =
        Message.new(%{
          id: "RAW-BAD-LAYOVER",
          from: "BRW",
          to: "OTZ",
          layover: "FAI",
          payload: "Unauthorized Transit"
        })

      :ok = :gen_tcp.send(sock, Message.encode(bad_msg))
      {:ok, ack_line} = :gen_tcp.recv(sock, 0, 2_000)
      :ok = :gen_tcp.close(sock)

      assert String.contains?(ack_line, "RAW-BAD-LAYOVER")
      assert_receive {:airport_event, "FAI", :rejected, {:unauthorized_transit, _detail}}, 2_000

      otz_state = "OTZ" |> AirportNode.via_tuple() |> AirportNode.get_state()
      assert otz_state.stats.arrived == 0
    end

    test "executes full 28-scenario suite across all 17 nodes and generates complete logs", %{
      codes: codes,
      log_dir: log_dir
    } do
      scenarios = Simulator.default_scenarios()
      results = Simulator.execute_scenarios(scenarios)

      expected_valid = Enum.count(scenarios, & &1.expect_valid?)
      expected_invalid = Enum.count(scenarios, &(not &1.expect_valid?))

      assert length(results.dispatched) == expected_valid
      assert map_size(results.arrived) == expected_valid
      assert length(results.rejected) == expected_invalid

      # Ensure every airport participated in sending or receiving
      snapshots = Simulator.collect_snapshots(codes)

      Enum.each(snapshots, fn snap ->
        assert snap.stats.sent + snap.stats.arrived + snap.stats.forwarded > 0
      end)

      # Stop network cleanly via ExUnit supervisor before asserting shutdown logs and socket release
      assert :ok == stop_supervised!(Simulator)

      sim_log = Path.join(log_dir, "simulation.log")
      assert File.exists?(sim_log)
      sim_content = File.read!(sim_log)

      for marker <- [
            "SOCKET_INIT",
            "CLIENT_CONNECT",
            "SOCKET_SEND",
            "SERVER_ACCEPT",
            "SOCKET_RECV",
            "HUB_ROUTING",
            "FINAL_ARRIVAL",
            "POLICY_REJECT",
            "NODE_SHUTDOWN",
            "SOCKET_CLOSE"
          ] do
        assert String.contains?(sim_content, marker), "Missing log marker #{marker}"
      end

      for code <- codes do
        node_log = Path.join(log_dir, "#{code}.log")
        assert File.exists?(node_log), "Missing per-airport log #{node_log}"
        content = File.read!(node_log)
        assert String.contains?(content, "SOCKET_INIT")
        assert String.contains?(content, "NODE_SHUTDOWN")
      end

      # Verify all 17 TCP ports were released cleanly on shutdown
      for info <- Directory.all_airports() do
        {:ok, ls} = :gen_tcp.listen(info.port, [:binary, reuseaddr: true, ip: info.ip])
        :ok = :gen_tcp.close(ls)
      end
    end
  end
end
