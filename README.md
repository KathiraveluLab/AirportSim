# AirportSim: Alaska Airlines Hub-and-Spoke Socket Simulation

AirportSim is an Elixir reference implementation of a peer-to-peer TCP socket network modeling passenger traffic for Alaska Airlines across 17 airports in Alaska and Seattle (`SEA`). Each airport operates as an independent peer process (`AirportSim.AirportNode`) equipped with both TCP server (listener/acceptor) and TCP client (connector/sender) capabilities built directly on Erlang/OTP's `:gen_tcp` socket interface.

Only two airports, Anchorage (`ANC`) and Seattle (`SEA`), act as transit hubs capable of inspecting packet headers and forwarding layover passengers to a final destination. The remaining 15 airports operate strictly as regional spoke endpoints.

---

## 1. System Architecture & Module Overview

- `AirportSim.Directory` (`lib/airport_sim/directory.ex`): Maintains the static lookup table mapping all 17 IATA airport codes to their IP addresses (`127.0.0.1`), TCP server ports (`5001` through `5017`), and network roles (`:hub` or `:spoke`). Enforces carrier scope (`Alaska Airlines` / `AS`), geographical scope (Alaska + `SEA` only), hub-only layover rules, and the prohibition on direct spoke-to-spoke flights.
- `AirportSim.Message` (`lib/airport_sim/message.ex`): Defines the passenger packet struct (`id`, `airline`, `from`, `to`, `layover`, `payload`, `hop_count`, `route_trace`, `timestamp`) and serializes/deserializes packets as newline-delimited JSON frames (`packet: :line`) over TCP streams using Elixir's built-in `JSON` module.
- `AirportSim.AirportNode` (`lib/airport_sim/airport_node.ex`): Implements each airport peer as a `GenServer` bound to a dedicated TCP port via `:gen_tcp.listen/2`. Each node runs an asynchronous acceptor loop (`:gen_tcp.accept/1`), sends application-layer `ACK` frames, routes or terminates incoming packets based on hub/spoke policy, and opens outbound `:gen_tcp.connect/4` client sockets to transmit or forward passengers.
- `AirportSim.FlightLogger` (`lib/airport_sim/flight_logger.ex`): Synchronously writes timestamped socket and routing lifecycle logs to both the unified `logs/simulation.log` and per-airport log files (`logs/<IATA>.log`).
- `AirportSim.Simulator` (`lib/airport_sim/simulator.ex`): OTP `Supervisor` and orchestrator that initializes the `Registry`, `FlightLogger`, and all 17 `AirportNode` peers, executes the 32-scenario verification suite, prints network summary metrics, and triggers clean socket shutdown.
- `AirportSim.CLI` & Mix Tasks (`lib/airport_sim/cli.ex`, `lib/mix/tasks/simulate.ex`, `lib/mix/tasks/airport.ex`): Provides `mix simulate`, `--route`, `--interactive`, `mix airport.node <IATA>`, and `mix airport.send` interfaces.

---

## 2. Question 1: How to Run the Program and Explicit Assumptions

### Requirements and Environment

- **Elixir**: `~> 1.18` (verified on Elixir `1.20.5`)
- **Erlang/OTP**: OTP `27+` (verified on Erlang/OTP `29`)
- **External Dependencies**: None. The project relies solely on the Elixir standard library (`JSON`, `GenServer`, `Supervisor`, `Registry`, `OptionParser`, `ExUnit`) and Erlang's `:gen_tcp` / `:inet` modules.
- **Network Ports**: TCP ports `5001` through `5017` on `127.0.0.1` must be free.

### Configuration and Execution Steps

1. **Compile the project**:
   ```bash
   mix compile --warnings-as-errors
   ```
   *(Note: If running on a restricted Windows or container filesystem that blocks hard-link lock files, prefix Mix commands with `MIX_OS_CONCURRENCY_LOCK=0`, e.g., `MIX_OS_CONCURRENCY_LOCK=0 mix compile`.)*

2. **Run the complete 17-airport, 32-scenario simulation**:
   ```bash
   mix simulate
   ```
   This initializes all 17 airport peer nodes, executes direct flights (`Spoke -> Hub`, `Hub -> Spoke`, `Hub <-> Hub`), one-layover flights (`Spoke -> ANC -> Spoke`, `Spoke -> SEA -> Spoke`, cross-hub layovers), and policy-rejection test cases (`BRW -> FAI -> OTZ`, `BRW -> OTZ`, `ANC -> LAX`, and `Delta Air Lines`), prints the per-airport summary table, writes `logs/simulation.log` and `logs/<IATA>.log`, and closes all listening sockets cleanly.

   To run silently to log files without console echo:
   ```bash
   mix simulate --quiet
   ```

3. **Run custom routes via command-line arguments**:
   ```bash
   mix simulate --route "FAI,BRW,ANC,Jane Doe" --route "BRW,ANC,NONE,John Smith"
   ```
   Route format is `"FROM,TO,LAYOVER,PASSENGER"` (use `NONE` or omit the layover field for direct hub-connected routes such as `"BRW,ANC,John Smith"`).

4. **Run interactive command-line input mode**:
   ```bash
   mix simulate --interactive
   ```
   Enter passenger routes interactively (`FROM,TO,LAYOVER,PASSENGER`), inspect live node metrics with `status` or `airports`, and terminate cleanly with `quit`, `exit`, or EOF (`Ctrl+D` on Unix/macOS, `Ctrl+Z` + Enter on Windows).

5. **Run individual peer nodes across separate terminals (multi-process OS mode)**:
   Open separate terminals for hubs and spokes:
   ```bash
   # Terminal 1 (Anchorage Hub)
   mix airport.node ANC

   # Terminal 2 (Utqiagvik Spoke)
   mix airport.node BRW

   # Terminal 3 (Fairbanks Spoke)
   mix airport.node FAI
   ```
   Inside Terminal 3 (`FAI>` prompt), dispatch a passenger through `ANC` to `BRW`:
   ```text
   send BRW ANC Alice Kugzruk
   ```
   Or dispatch from a fourth terminal using the standalone TCP client task:
   ```bash
   mix airport.send --from FAI --to BRW --layover ANC --passenger "Alice Kugzruk"
   ```

6. **Run the automated test suite**:
   ```bash
   mix test
   ```

### Airport Directory and Socket Lookup Table

All server ports exceed `5000` (`5001` through `5017`) in compliance with non-privileged port guidelines.

| IATA | Airport Name | Location | Role | Socket Address |
| :--- | :--- | :--- | :--- | :--- |
| `ANC` | Ted Stevens Anchorage International Airport | Anchorage, AK | Hub | `127.0.0.1:5001` |
| `SEA` | Seattle-Tacoma International Airport | Seattle, WA | Hub | `127.0.0.1:5002` |
| `FAI` | Fairbanks International Airport | Fairbanks, AK | Spoke | `127.0.0.1:5003` |
| `JNU` | Juneau International Airport | Juneau, AK | Spoke | `127.0.0.1:5004` |
| `SIT` | Sitka Rocky Gutierrez Airport | Sitka, AK | Spoke | `127.0.0.1:5005` |
| `ADK` | Adak Airport | Adak, AK | Spoke | `127.0.0.1:5006` |
| `ADQ` | Kodiak Airport | Kodiak, AK | Spoke | `127.0.0.1:5007` |
| `OTZ` | Ralph Wien Memorial Airport | Kotzebue, AK | Spoke | `127.0.0.1:5008` |
| `NME` | Nightmute Airport | Nightmute, AK | Spoke | `127.0.0.1:5009` |
| `BET` | Bethel Airport | Bethel, AK | Spoke | `127.0.0.1:5010` |
| `AKN` | King Salmon Airport | King Salmon, AK | Spoke | `127.0.0.1:5011` |
| `DLG` | Dillingham Airport | Dillingham, AK | Spoke | `127.0.0.1:5012` |
| `BRW` | Wiley Post-Will Rogers Memorial Airport | Utqiagvik, AK | Spoke | `127.0.0.1:5013` |
| `SCC` | Deadhorse Airport | Prudhoe Bay, AK | Spoke | `127.0.0.1:5014` |
| `CDV` | Merle K. (Mudhole) Smith Airport | Cordova, AK | Spoke | `127.0.0.1:5015` |
| `DUT` | Unalaska Airport | Dutch Harbor, AK | Spoke | `127.0.0.1:5016` |
| `KTN` | Ketchikan International Airport | Ketchikan, AK | Spoke | `127.0.0.1:5017` |

### Explicit Design Assumptions

1. **Single Carrier Scope**: Only `Alaska Airlines` (`"AS"`) passenger traffic is valid. Any packet declaring another airline is rejected by policy (`POLICY_REJECT`) before transmission and on arrival.
2. **Closed 17-Airport Graph**: Traffic originating from or destined to airports outside the 17 listed Alaska/Seattle nodes (e.g., `LAX`, `ORD`) is rejected.
3. **Strict Two-Hub Constraint**: Only `ANC` and `SEA` may serve as transit/layover hubs. Both origin-side validation and server-side packet inspection reject non-hub transit attempts (e.g., `FAI` acting as a layover).
4. **Maximum One Layover and No Spoke-to-Spoke Direct Links**: A passenger travels either in 1 hop (involving at least one hub) or in 2 hops (`Origin -> Hub -> Destination`). Direct routes between two spoke airports without a hub layover are rejected unless automatic layover resolution (`:auto`) is explicitly requested by the caller.
5. **TCP Stream Framing and Acknowledgment**: Because TCP is a byte-stream protocol without intrinsic message boundaries, packets are framed as newline-terminated single-line JSON strings using `:gen_tcp` with `packet: :line`. Each receiving node responds with an application-level JSON `ACK` before closing the accepted connection socket.
6. **Localhost Loopback Deployment**: All 17 peers bind to `127.0.0.1` on distinct ports (`5001` through `5017`) so the entire network runs deterministically on a single machine, while keeping the IP address field modular for multi-host deployment.

---

## 3. Parallels with Internet eXchange Points (IXPs) and Hub-and-Spoke Simulation

The 17-airport Alaska Airlines network modeled in AirportSim mirrors core architectural patterns found in Internet routing and Internet eXchange Points (IXPs):

1. **Hubs as IXPs and Tier-1 Transit Core**: Regional Alaskan communities (`BRW`, `OTZ`, `ADK`, `NME`, `BET`, `DUT`, `JNU`, `KTN`, etc.) behave like stub or edge Autonomous Systems (ASes) that lack direct physical peering links to one another. Instead of maintaining an $O(N^2)$ full mesh, where 17 airports would require $17 \times 16 / 2 = 136$ bidirectional point-to-point links, edge nodes establish direct circuits to core aggregation hubs (`ANC` and `SEA`). In Internet topology, IXPs and regional Tier-1/Tier-2 transit providers play the exact same role: multiple edge networks interconnect at a shared switching fabric (`ANC` or `SEA`) to exchange traffic across distant regions without running dedicated fibers between every pair of cities.
2. **Packet Encapsulation and Header Inspection**: Every transmitted `AirportSim.Message` separates control-plane routing headers (`from`, `to`, `layover`, `airline`, `hop_count`, `route_trace`) from the opaque user data (`payload`, representing the passenger identity). When a packet arrives on `ANC`'s TCP listening socket (`127.0.0.1:5001`), `ANC` parses the header just as an IP router or BGP border gateway inspects a packet/frame destination:
   - If `msg.to == "ANC"`, the node acts as the destination host, delivers the payload to the application layer (`FINAL_ARRIVAL`), and terminates forwarding.
   - If `msg.layover == "ANC"` and `msg.to != "ANC"`, `ANC` acts as an intermediate router/transit switch (`HUB_ROUTING`): it increments `hop_count`, appends `"ANC"` to `route_trace`, consults its forwarding table (`AirportSim.Directory`) to resolve the destination's `IP:Port`, opens a new outbound TCP client socket to `msg.to`, and forwards the packet.
3. **Overlay Routing over a Physical Underlay**: Even though all 17 processes communicate over local TCP/IP sockets (`127.0.0.1`) that could physically reach each other directly at Layer 3/4, the application layer enforces a strict logical overlay topology. Spoke nodes reject any packet requesting transit (`unauthorized_transit`), guaranteeing that multi-hop traffic physically traverses the `Origin -> Hub -> Destination` overlay path just as BGP policies constrain AS paths across the global Internet.

---

## 4. Reflections, Scalability, and Caveats in Computer Networks



### Networking Assumptions

- **Reliable In-Order Byte-Stream Transport**: Each flight leg runs over TCP (`:gen_tcp`) rather than UDP, assuming reliable, ordered delivery with application-level `ACK` confirmation prior to closing each connection socket.
- **Flat Localhost Overlay Mapped to Logical Topology**: All 17 nodes share a single loopback subnet (`127.0.0.1`) with distinct non-privileged TCP ports (`5001` through `5017`) while enforcing logical hub-and-spoke reachability constraints at the application layer.
- **Fixed Static Forwarding State**: Routing relies on static lookup entries in `AirportSim.Directory` and explicit single-layover headers (`ANC` or `SEA`) rather than dynamic link-state or path-vector protocol discovery, assuming zero node failures during a simulation run.
- **Unbounded Node Buffers with Synchronous Logging**: Concurrent incoming TCP connections are handled by independent lightweight BEAM processes, while `AirportSim.FlightLogger` serializes file writes synchronously to guarantee deterministic chronological ordering.

### Transport-Layer Mechanics Observed

- **Peer Duality (Server + Client per Node)**: Every `AirportNode` simultaneously maintains a long-lived passive listening socket (`:gen_tcp.listen/2` with `reuseaddr: true`) and creates short-lived active client sockets (`:gen_tcp.connect/4`) on demand. Observing OS-assigned ephemeral client ports (`:inet.sockname/1`) alongside fixed well-known server ports (`5001` through `5017`) directly demonstrates TCP 4-tuple demultiplexing (`{src_ip, src_port, dst_ip, dst_port}`).
- **Message Framing over TCP**: Raw TCP streams can coalesce multiple small writes or split a single JSON payload across `recv` calls. Configuring `:gen_tcp` with `packet: :line` and `active: false` ensures deterministic message boundary detection and built-in backpressure.
- **Resource Lifecycle Management**: Failing to close accepted or client sockets exhausts file descriptors and leaves sockets in `TIME_WAIT` or bound states. `AirportSim.AirportNode` wraps every client and server socket interaction in `try ... after :gen_tcp.close(socket)` and traps supervisor exit signals so `listen_socket` handles are explicitly closed upon termination of input (`NODE_SHUTDOWN`).

### Architectural Trade-Offs and Network Caveats

- **Centralization Bottlenecks & Path Inflation**: Routing `OTZ -> ANC -> NME` or `JNU -> SEA -> SIT` concentrates forwarding load on `ANC` and `SEA`. While link maintenance drops sharply compared to a full mesh, hub failures become high-blast-radius outages (single points of failure), and geographically proximate spokes suffer path inflation (extra hop latency through a distant hub).
- **Scalability Characteristics**:
  - *Process Concurrency*: Because Erlang/OTP runs each `AirportNode`, acceptor loop, and connection handler inside lightweight BEAM processes rather than heavy OS threads, scaling from 17 peers to hundreds of simulated airports on one laptop incurs minimal memory overhead.
  - *Distributed Multi-Machine Scaling*: Updating the `{ip, ip_str, port}` entries in `AirportSim.Directory` from `127.0.0.1` to physical host IPv4 addresses allows nodes to execute across separate machines on a LAN or WAN without changing socket transport code.
  - *Logging Serialization Caveat*: In single-VM mode (`mix simulate`), `AirportSim.FlightLogger` serializes writes through one `GenServer` to guarantee clean chronological ordering in `logs/simulation.log`. Under extreme message throughput across thousands of concurrent flights, a single synchronous logger becomes a throughput bottleneck and should be replaced by asynchronous log buffering or per-node local log files.
  - *Static vs. Dynamic Routing*: The current implementation relies on a static directory (`AirportSim.Directory`) and explicit layover selection (`ANC` or `SEA`). In real computer networks, static tables break down when links fail or topologies change, motivating dynamic routing protocols (OSPF, BGP) and redundant multi-homed peering.

---

## 5. Log Files and Output Format

Running `mix simulate` generates:
- `logs/simulation.log`: Complete chronological trace of all socket and routing events across the 17-node network.
- `logs/<IATA>.log`: Individual activity log for each airport (`ANC.log`, `SEA.log`, `FAI.log`, ..., `KTN.log`).
- `logs/SYSTEM.log`: Orchestrator startup, scenario dispatch, summary, and shutdown markers.

Key log event tags:
- `SOCKET_INIT`: Peer bound its listening TCP socket on `127.0.0.1:<port>`.
- `CLIENT_CONNECT`: Peer initiated an outbound TCP connection to the next hop.
- `SOCKET_SEND`: Origin or hub transmitted a serialized JSON passenger packet, recording its ephemeral client socket and target `IP:Port`.
- `SERVER_ACCEPT`: Listening node accepted an incoming TCP connection from a peer.
- `SOCKET_RECV`: Node received a passenger packet or application-level `ACK`.
- `HUB_ROUTING`: Hub (`ANC` or `SEA`) inspected packet headers, identified itself as a layover transit point, and spawned forwarding to the destination.
- `FINAL_ARRIVAL`: Destination node verified `msg.to == node.iata` and logged completion (`direct` or `layover` path trace).
- `POLICY_REJECT`: Origin or server node blocked a route violating carrier, scope, or hub-only transit constraints.
- `NODE_SHUTDOWN` / `SOCKET_CLOSE`: Node detected termination of input and closed its TCP sockets cleanly.
