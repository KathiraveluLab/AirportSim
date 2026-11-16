defmodule AirportSim.AirportNode do
  @moduledoc """
  Peer GenServer representing a single airport node with both TCP Server (listener/acceptor)
  and TCP Client (connector/sender) capabilities via Erlang `:gen_tcp`.

  Hub nodes (`ANC` and `SEA`) inspect incoming passenger headers to decide whether they are
  the final destination or an intermediate layover point requiring forwarding over a new TCP
  client connection. Spoke nodes only accept traffic addressed to themselves as final
  destination and drop unauthorized transit attempts.
  """

  use GenServer, restart: :transient

  alias AirportSim.{Directory, FlightLogger, Message}

  @tcp_timeout 5_000

  defstruct [
    :iata,
    :info,
    :port,
    :listen_socket,
    :acceptor_pid,
    :logger,
    subscribers: [],
    sent: [],
    forwarded: [],
    arrived: [],
    rejected: [],
    stats: %{sent: 0, forwarded: 0, received: 0, arrived: 0, rejected: 0}
  ]

  @doc """
  Starts an AirportNode GenServer and binds its TCP listening socket.

  Options:
    - `:iata` (required) - 3-letter airport IATA code
    - `:logger` - FlightLogger server name or PID (defaults to `AirportSim.FlightLogger`)
    - `:port` - optional port override (defaults to `Directory` port)
    - `:name` - optional GenServer registration name
    - `:subscribers` - optional list of PIDs to receive `{:airport_event, iata, event, data}`
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    iata = opts |> Keyword.fetch!(:iata) |> Directory.normalize_code()
    gen_opts = if name = Keyword.get(opts, :name), do: [name: name], else: []
    GenServer.start_link(__MODULE__, Keyword.put(opts, :iata, iata), gen_opts)
  end

  @doc "Returns the Registry `:via` tuple for an airport IATA code."
  @spec via_tuple(String.t()) :: {:via, Registry, {AirportSim.NodeRegistry, String.t()}}
  def via_tuple(iata) do
    {:via, Registry, {AirportSim.NodeRegistry, Directory.normalize_code(iata)}}
  end

  @doc "Gracefully stops an AirportNode, closing its listening TCP socket."
  @spec stop(GenServer.server()) :: :ok
  def stop(node) do
    GenServer.stop(node, :normal, @tcp_timeout)
  catch
    :exit, _ -> :ok
  end

  @doc "Subscribes a caller PID to node routing events (`:sent`, `:forwarded`, `:arrived`, `:rejected`)."
  @spec subscribe(GenServer.server(), pid()) :: :ok
  def subscribe(node, pid \\ self()) do
    GenServer.call(node, {:subscribe, pid}, @tcp_timeout)
  end

  @doc "Returns a snapshot of the node's statistics and message lists."
  @spec get_state(GenServer.server()) :: map()
  def get_state(node) do
    GenServer.call(node, :get_state, @tcp_timeout)
  end

  @doc """
  Initiates sending a passenger message from this airport node over TCP.

  Attributes map/keyword may contain:
    - `:to` (required) - destination IATA code
    - `:payload` (required) - passenger name / identifying detail
    - `:layover` (optional) - hub IATA code (`"ANC"`, `"SEA"`, `nil`, or `:auto`)
    - `:airline` (optional) - defaults to `"Alaska Airlines"`
    - `:id` (optional) - custom message ID
  """
  @spec send_passenger(GenServer.server(), keyword() | map()) ::
          {:ok, Message.t()} | {:error, {atom(), String.t()}}
  def send_passenger(node, attrs) when is_list(attrs), do: send_passenger(node, Map.new(attrs))

  def send_passenger(node, attrs) when is_map(attrs) do
    GenServer.call(node, {:send_passenger, attrs}, 15_000)
  end

  @doc """
  Standalone TCP client transmission function usable from CLI commands without booting
  a local origin GenServer. Validates route rules and sends the passenger packet over `:gen_tcp`.
  """
  @spec transmit_standalone(map() | keyword(), GenServer.server() | nil) ::
          {:ok, Message.t()} | {:error, {atom(), String.t()}}
  def transmit_standalone(attrs, logger \\ FlightLogger) when is_list(attrs) or is_map(attrs) do
    attrs = Map.new(attrs)
    origin = Directory.normalize_code(Map.fetch!(attrs, :from))
    to = Directory.normalize_code(Map.fetch!(attrs, :to))
    layover_opt = Map.get(attrs, :layover)
    airline = Map.get(attrs, :airline, Directory.default_airline())
    payload = Map.fetch!(attrs, :payload)

    case Directory.resolve_route(origin, to, layover_opt, airline) do
      {:ok, resolved_layover} ->
        msg =
          Message.new(%{
            id: Map.get(attrs, :id),
            airline: airline,
            from: origin,
            to: to,
            layover: resolved_layover,
            payload: payload
          })

        next_hop = if msg.layover, do: msg.layover, else: msg.to

        case transmit_over_tcp(origin, next_hop, msg, logger) do
          :ok -> {:ok, msg}
          {:error, reason} -> {:error, {:tcp_error, inspect(reason)}}
        end

      {:error, reason, detail} ->
        if logger, do: FlightLogger.log(origin || "SYSTEM", "POLICY_REJECT", detail, logger)
        {:error, {reason, detail}}
    end
  end

  # GenServer Callbacks

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    iata = Keyword.fetch!(opts, :iata)
    logger = Keyword.get(opts, :logger, FlightLogger)
    subscribers = Keyword.get(opts, :subscribers, [])

    case Directory.get_airport(iata) do
      {:ok, info} ->
        port = Keyword.get(opts, :port, info.port)
        listen_opts = [:binary, packet: :line, active: false, reuseaddr: true, ip: info.ip]

        case :gen_tcp.listen(port, listen_opts) do
          {:ok, listen_socket} ->
            role_label = if info.role == :hub, do: "HUB", else: "SPOKE"

            FlightLogger.log(
              iata,
              "SOCKET_INIT",
              "Initialized #{role_label} peer node #{iata} (#{info.name}); listening on #{info.ip_str}:#{port}.",
              logger
            )

            parent = self()
            acceptor_pid = spawn_link(fn -> accept_loop(listen_socket, iata, parent, logger) end)

            {:ok,
             %__MODULE__{
               iata: iata,
               info: info,
               port: port,
               listen_socket: listen_socket,
               acceptor_pid: acceptor_pid,
               logger: logger,
               subscribers: subscribers
             }}

          {:error, reason} ->
            {:stop, {:listen_failed, iata, port, reason}}
        end

      {:error, :unknown_airport} ->
        {:stop, {:unknown_airport, iata}}
    end
  end

  @impl true
  def handle_call({:subscribe, pid}, _from, state) do
    subscribers = Enum.uniq([pid | state.subscribers])
    {:reply, :ok, %{state | subscribers: subscribers}}
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    snapshot = %{
      iata: state.iata,
      role: state.info.role,
      address: "#{state.info.ip_str}:#{state.port}",
      stats: state.stats,
      sent: Enum.reverse(state.sent),
      forwarded: Enum.reverse(state.forwarded),
      arrived: Enum.reverse(state.arrived),
      rejected: Enum.reverse(state.rejected)
    }

    {:reply, snapshot, state}
  end

  @impl true
  def handle_call({:send_passenger, attrs}, _from, state) do
    to = Map.fetch!(attrs, :to)
    layover_opt = Map.get(attrs, :layover)
    airline = Map.get(attrs, :airline, Directory.default_airline())
    payload = Map.fetch!(attrs, :payload)

    case Directory.resolve_route(state.iata, to, layover_opt, airline) do
      {:ok, resolved_layover} ->
        msg =
          Message.new(%{
            id: Map.get(attrs, :id),
            airline: airline,
            from: state.iata,
            to: to,
            layover: resolved_layover,
            payload: payload
          })

        next_hop = if msg.layover, do: msg.layover, else: msg.to

        case transmit_over_tcp(state.iata, next_hop, msg, state.logger) do
          :ok ->
            updated =
              state
              |> Map.update!(:sent, &[msg | &1])
              |> put_in([Access.key!(:stats), :sent], state.stats.sent + 1)

            notify_subscribers(updated, :sent, msg)
            {:reply, {:ok, msg}, updated}

          {:error, reason} ->
            detail =
              "TCP transmission from #{state.iata} to #{next_hop} failed: #{inspect(reason)}."

            FlightLogger.log(state.iata, "SOCKET_ERROR", detail, state.logger)

            updated =
              state
              |> Map.update!(:rejected, &[{msg, {:tcp_error, detail}} | &1])
              |> put_in([Access.key!(:stats), :rejected], state.stats.rejected + 1)

            notify_subscribers(updated, :rejected, {{:tcp_error, detail}, msg})
            {:reply, {:error, {:tcp_error, detail}}, updated}
        end

      {:error, reason, detail} ->
        FlightLogger.log(
          state.iata,
          "POLICY_REJECT",
          "Blocked passenger #{inspect(payload)} on #{state.iata} -> #{inspect(to)} (layover: #{inspect(layover_opt)}, airline: #{inspect(airline)}): #{detail}",
          state.logger
        )

        updated =
          state
          |> Map.update!(:rejected, &[{attrs, {reason, detail}} | &1])
          |> put_in([Access.key!(:stats), :rejected], state.stats.rejected + 1)

        notify_subscribers(updated, :rejected, {reason, detail})
        {:reply, {:error, {reason, detail}}, updated}
    end
  end

  @impl true
  def handle_call({:process_incoming, %Message{} = msg, peer_str}, _from, state) do
    state = put_in(state, [Access.key!(:stats), :received], state.stats.received + 1)
    updated = route_incoming_message(msg, peer_str, state)
    {:reply, :ok, updated}
  end

  @impl true
  def handle_cast({:forward_complete, forwarded_msg, :ok}, state) do
    updated =
      state
      |> Map.update!(:forwarded, &[forwarded_msg | &1])
      |> put_in([Access.key!(:stats), :forwarded], state.stats.forwarded + 1)

    notify_subscribers(updated, :forwarded, forwarded_msg)
    {:noreply, updated}
  end

  @impl true
  def handle_cast({:forward_complete, forwarded_msg, {:error, reason}}, state) do
    detail =
      "Hub #{state.iata} failed to forward #{forwarded_msg.id} to #{forwarded_msg.to}: #{inspect(reason)}."

    FlightLogger.log(state.iata, "SOCKET_ERROR", detail, state.logger)

    updated =
      state
      |> Map.update!(:rejected, &[{forwarded_msg, {:forward_failed, detail}} | &1])
      |> put_in([Access.key!(:stats), :rejected], state.stats.rejected + 1)

    notify_subscribers(updated, :rejected, {:forward_failed, detail})
    {:noreply, updated}
  end

  @impl true
  def handle_info({:EXIT, _pid, :normal}, state) do
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, :shutdown}, state) do
    {:noreply, state}
  end

  def handle_info({:EXIT, pid, reason}, state) do
    FlightLogger.log(
      state.iata,
      "WORKER_EXIT",
      "Linked process #{inspect(pid)} exited unexpectedly: #{inspect(reason)}.",
      state.logger
    )

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    FlightLogger.log(
      state.iata,
      "NODE_SHUTDOWN",
      "Detected termination of input for #{state.iata}; shutting down TCP server on #{state.info.ip_str}:#{state.port}.",
      state.logger
    )

    if state.listen_socket do
      :gen_tcp.close(state.listen_socket)

      FlightLogger.log(
        state.iata,
        "SOCKET_CLOSE",
        "Closed listening TCP socket #{state.info.ip_str}:#{state.port}.",
        state.logger
      )
    end

    :ok
  end

  # Incoming Message Routing Engine

  defp route_incoming_message(%Message{} = msg, peer_str, state) do
    cond do
      msg.airline not in ["Alaska Airlines", "AS"] ->
        detail =
          "Dropped #{msg.id} from #{peer_str}: unsupported carrier #{inspect(msg.airline)}."

        record_server_reject(state, msg, :unsupported_airline, detail)

      not Directory.valid_airport?(msg.from) or not Directory.valid_airport?(msg.to) ->
        detail =
          "Dropped #{msg.id} from #{peer_str}: invalid endpoint #{msg.from} -> #{msg.to}."

        record_server_reject(state, msg, :unknown_airport, detail)

      msg.to == state.iata ->
        arrived_msg = Message.mark_arrived(msg, state.iata)
        path_str = Enum.join(arrived_msg.route_trace, " -> ")
        arrival_kind = if arrived_msg.hop_count > 1, do: "layover", else: "direct"

        FlightLogger.log(
          state.iata,
          "FINAL_ARRIVAL",
          "Passenger \"#{arrived_msg.payload}\" (#{arrived_msg.id}) arrived at final destination #{state.iata} via #{arrival_kind} path [#{path_str}] (hops: #{arrived_msg.hop_count}).",
          state.logger
        )

        updated =
          state
          |> Map.update!(:arrived, &[arrived_msg | &1])
          |> put_in([Access.key!(:stats), :arrived], state.stats.arrived + 1)

        notify_subscribers(updated, :arrived, arrived_msg)
        updated

      msg.layover == state.iata and Directory.hub?(state.iata) ->
        dest_addr = Directory.address_for(msg.to)

        FlightLogger.log(
          state.iata,
          "HUB_ROUTING",
          "Hub #{state.iata} interpreted #{msg.id} (from: #{msg.from}, layover: #{msg.layover}, to: #{msg.to}, passenger: \"#{msg.payload}\"): acting as layover transit, forwarding to final destination #{msg.to} (#{dest_addr}).",
          state.logger
        )

        forwarded_msg = Message.forward_from_hub(msg, state.iata)
        node_pid = self()
        hub_iata = state.iata
        logger = state.logger

        spawn_link(fn ->
          res = transmit_over_tcp(hub_iata, forwarded_msg.to, forwarded_msg, logger)
          GenServer.cast(node_pid, {:forward_complete, forwarded_msg, res})
        end)

        state

      true ->
        detail =
          "Node #{state.iata} (#{state.info.role}) rejected transit of #{msg.id} destined for #{msg.to} (layover: #{inspect(msg.layover)}); non-hub airports cannot serve as transit points."

        record_server_reject(state, msg, :unauthorized_transit, detail)
    end
  end

  defp record_server_reject(state, msg, reason, detail) do
    FlightLogger.log(state.iata, "POLICY_REJECT", detail, state.logger)

    updated =
      state
      |> Map.update!(:rejected, &[{msg, {reason, detail}} | &1])
      |> put_in([Access.key!(:stats), :rejected], state.stats.rejected + 1)

    notify_subscribers(updated, :rejected, {reason, detail})
    updated
  end

  defp notify_subscribers(state, event, data) do
    Enum.each(state.subscribers, fn pid ->
      send(pid, {:airport_event, state.iata, event, data})
    end)
  end

  # TCP Server Acceptor Loop & Connection Handler

  defp accept_loop(listen_socket, iata, node_pid, logger) do
    case :gen_tcp.accept(listen_socket) do
      {:ok, client_socket} ->
        spawn(fn -> handle_client_socket(client_socket, iata, node_pid, logger) end)
        accept_loop(listen_socket, iata, node_pid, logger)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        FlightLogger.log(
          iata,
          "SOCKET_ERROR",
          "Acceptor loop error on #{iata}: #{inspect(reason)}.",
          logger
        )

        :ok
    end
  end

  defp handle_client_socket(client_socket, iata, node_pid, logger) do
    peer_str = format_peername(client_socket)

    FlightLogger.log(
      iata,
      "SERVER_ACCEPT",
      "Accepted incoming TCP connection from peer #{peer_str}.",
      logger
    )

    try do
      case :gen_tcp.recv(client_socket, 0, @tcp_timeout) do
        {:ok, data} ->
          case Message.decode(data) do
            {:ok, msg} ->
              FlightLogger.log(
                iata,
                "SOCKET_RECV",
                "Received packet #{msg.id} (from: #{msg.from}, layover: #{inspect(msg.layover)}, to: #{msg.to}, hop: #{msg.hop_count}, passenger: \"#{msg.payload}\") from #{peer_str}.",
                logger
              )

              GenServer.call(node_pid, {:process_incoming, msg, peer_str}, @tcp_timeout)

              ack =
                JSON.encode!(%{"status" => "ACK", "node" => iata, "msg_id" => msg.id}) <> "\n"

              :ok = :gen_tcp.send(client_socket, ack)

            {:error, decode_reason} ->
              FlightLogger.log(
                iata,
                "SOCKET_ERROR",
                "Malformed payload from #{peer_str}: #{inspect(decode_reason)}.",
                logger
              )

              nack =
                JSON.encode!(%{"status" => "NACK", "node" => iata, "error" => "invalid_json"}) <>
                  "\n"

              :gen_tcp.send(client_socket, nack)
          end

        {:error, :closed} ->
          FlightLogger.log(
            iata,
            "SOCKET_RECV",
            "Peer #{peer_str} closed connection before sending data.",
            logger
          )

        {:error, reason} ->
          FlightLogger.log(
            iata,
            "SOCKET_ERROR",
            "TCP recv error from #{peer_str}: #{inspect(reason)}.",
            logger
          )
      end
    after
      :gen_tcp.close(client_socket)

      FlightLogger.log(
        iata,
        "SOCKET_CLOSE",
        "Closed accepted server-side client socket from #{peer_str}.",
        logger
      )
    end
  end

  # TCP Client Sender

  defp transmit_over_tcp(sender_iata, target_iata, %Message{} = msg, logger) do
    {:ok, target_info} = Directory.get_airport(target_iata)
    connect_opts = [:binary, packet: :line, active: false]

    FlightLogger.log(
      sender_iata,
      "CLIENT_CONNECT",
      "Connecting TCP client from #{sender_iata} to #{target_iata} (#{target_info.ip_str}:#{target_info.port}) for #{msg.id}.",
      logger
    )

    case :gen_tcp.connect(target_info.ip, target_info.port, connect_opts, @tcp_timeout) do
      {:ok, sock} ->
        client_str = format_sockname(sock)

        try do
          encoded = Message.encode(msg)

          FlightLogger.log(
            sender_iata,
            "SOCKET_SEND",
            "Sending #{msg.id} (route: #{Message.format_route(msg)}, hop: #{msg.hop_count}, passenger: \"#{msg.payload}\") via #{client_str} -> #{target_info.ip_str}:#{target_info.port}.",
            logger
          )

          with :ok <- :gen_tcp.send(sock, encoded),
               {:ok, ack_line} <- :gen_tcp.recv(sock, 0, @tcp_timeout) do
            FlightLogger.log(
              sender_iata,
              "SOCKET_RECV",
              "Received #{String.trim(ack_line)} from #{target_iata} for #{msg.id}.",
              logger
            )

            :ok
          end
        after
          :gen_tcp.close(sock)

          FlightLogger.log(
            sender_iata,
            "SOCKET_CLOSE",
            "Closed TCP client socket #{client_str} after sending #{msg.id}.",
            logger
          )
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp format_peername(socket) do
    case :inet.peername(socket) do
      {:ok, {ip, port}} -> "#{:inet.ntoa(ip)}:#{port}"
      {:error, _} -> "unknown_peer"
    end
  end

  defp format_sockname(socket) do
    case :inet.sockname(socket) do
      {:ok, {ip, port}} -> "#{:inet.ntoa(ip)}:#{port}"
      {:error, _} -> "unknown_local"
    end
  end
end
