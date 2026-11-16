defmodule AirportSim.Message do
  @moduledoc """
  Represents a passenger routing packet transmitted over newline-delimited TCP sockets.
  Encapsulates the origin (`from`), destination (`to`), optional hub (`layover`),
  passenger identification (`payload`), airline carrier, hop counter, and visited node trace.
  """

  alias AirportSim.Directory

  @enforce_keys [:id, :from, :to, :payload]
  defstruct [
    :id,
    :from,
    :to,
    :layover,
    :payload,
    airline: "Alaska Airlines",
    hop_count: 1,
    route_trace: [],
    timestamp: nil
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          airline: String.t(),
          from: String.t(),
          to: String.t(),
          layover: String.t() | nil,
          payload: String.t(),
          hop_count: pos_integer(),
          route_trace: [String.t()],
          timestamp: String.t() | nil
        }

  @doc "Builds a new passenger message struct initialized at the origin airport."
  @spec new(keyword() | map()) :: t()
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    from = Directory.normalize_code(Map.fetch!(attrs, :from))
    to = Directory.normalize_code(Map.fetch!(attrs, :to))
    layover = Directory.normalize_code(Map.get(attrs, :layover))
    id = Map.get(attrs, :id) || generate_id()
    airline = Map.get(attrs, :airline) || Directory.default_airline()
    payload = Map.fetch!(attrs, :payload)
    hop_count = Map.get(attrs, :hop_count, 1)
    route_trace = Map.get(attrs, :route_trace, [from])
    timestamp = Map.get(attrs, :timestamp) || DateTime.utc_now() |> DateTime.to_iso8601()

    %__MODULE__{
      id: id,
      airline: airline,
      from: from,
      to: to,
      layover: layover,
      payload: payload,
      hop_count: hop_count,
      route_trace: route_trace,
      timestamp: timestamp
    }
  end

  @doc "Serializes a message into a single-line JSON string terminated with a newline for TCP framing."
  @spec encode(t()) :: String.t()
  def encode(%__MODULE__{} = msg) do
    payload_map = %{
      "id" => msg.id,
      "airline" => msg.airline,
      "from" => msg.from,
      "to" => msg.to,
      "layover" => msg.layover,
      "payload" => msg.payload,
      "hop_count" => msg.hop_count,
      "route_trace" => msg.route_trace,
      "timestamp" => msg.timestamp
    }

    JSON.encode!(payload_map) <> "\n"
  end

  @doc "Decodes a line-delimited JSON string received from a TCP socket into an AirportSim.Message struct."
  @spec decode(String.t()) :: {:ok, t()} | {:error, term()}
  def decode(line) when is_binary(line) do
    trimmed = String.trim(line)

    case JSON.decode(trimmed) do
      {:ok, map} when is_map(map) ->
        with {:ok, id} <- fetch_required_string(map, "id"),
             {:ok, from} <- fetch_required_string(map, "from"),
             {:ok, to} <- fetch_required_string(map, "to"),
             {:ok, payload} <- fetch_required_string(map, "payload") do
          {:ok,
           %__MODULE__{
             id: id,
             airline: Map.get(map, "airline", Directory.default_airline()),
             from: Directory.normalize_code(from),
             to: Directory.normalize_code(to),
             layover: Directory.normalize_code(Map.get(map, "layover")),
             payload: payload,
             hop_count: Map.get(map, "hop_count", 1),
             route_trace: Map.get(map, "route_trace", []),
             timestamp: Map.get(map, "timestamp")
           }}
        end

      {:ok, _other} ->
        {:error, :invalid_message_format}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Formats a route string (e.g., \"FAI -> ANC -> BRW\" or \"BRW -> ANC\")."
  @spec format_route(t()) :: String.t()
  def format_route(%__MODULE__{from: from, layover: nil, to: to}), do: "#{from} -> #{to}"

  def format_route(%__MODULE__{from: from, layover: layover, to: to}),
    do: "#{from} -> #{layover} -> #{to}"

  @doc "Advances a message when forwarded by an intermediate layover hub."
  @spec forward_from_hub(t(), String.t()) :: t()
  def forward_from_hub(%__MODULE__{} = msg, hub_iata) do
    %{
      msg
      | hop_count: msg.hop_count + 1,
        route_trace: msg.route_trace ++ [hub_iata],
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
    }
  end

  @doc "Marks a message as arrived at its final destination airport."
  @spec mark_arrived(t(), String.t()) :: t()
  def mark_arrived(%__MODULE__{} = msg, dest_iata) do
    %{
      msg
      | route_trace: msg.route_trace ++ [dest_iata],
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
    }
  end

  defp fetch_required_string(map, key) do
    case Map.get(map, key) do
      val when is_binary(val) and byte_size(val) > 0 -> {:ok, val}
      _ -> {:error, {:missing_field, key}}
    end
  end

  defp generate_id do
    suffix = System.unique_integer([:positive, :monotonic])
    "MSG-#{String.pad_leading(Integer.to_string(suffix), 4, "0")}"
  end
end
