defmodule AirportSim.Directory do
  @moduledoc """
  Static airport directory, IP:Port lookup table, and hub-and-spoke routing policy validator
  for the 17 Alaska Airlines airports (2 hubs and 15 spokes).
  """

  @default_airline "Alaska Airlines"
  @allowed_airlines ["Alaska Airlines", "AS"]

  @type role :: :hub | :spoke
  @type airport_info :: %{
          iata: String.t(),
          name: String.t(),
          city: String.t(),
          ip: :inet.ip_address(),
          ip_str: String.t(),
          port: pos_integer(),
          role: role()
        }

  @airports %{
    "ANC" => %{
      iata: "ANC",
      name: "Ted Stevens Anchorage International Airport",
      city: "Anchorage, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5001,
      role: :hub
    },
    "SEA" => %{
      iata: "SEA",
      name: "Seattle-Tacoma International Airport",
      city: "Seattle, WA",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5002,
      role: :hub
    },
    "FAI" => %{
      iata: "FAI",
      name: "Fairbanks International Airport",
      city: "Fairbanks, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5003,
      role: :spoke
    },
    "JNU" => %{
      iata: "JNU",
      name: "Juneau International Airport",
      city: "Juneau, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5004,
      role: :spoke
    },
    "SIT" => %{
      iata: "SIT",
      name: "Sitka Rocky Gutierrez Airport",
      city: "Sitka, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5005,
      role: :spoke
    },
    "ADK" => %{
      iata: "ADK",
      name: "Adak Airport",
      city: "Adak, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5006,
      role: :spoke
    },
    "ADQ" => %{
      iata: "ADQ",
      name: "Kodiak Airport",
      city: "Kodiak, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5007,
      role: :spoke
    },
    "OTZ" => %{
      iata: "OTZ",
      name: "Ralph Wien Memorial Airport",
      city: "Kotzebue, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5008,
      role: :spoke
    },
    "NME" => %{
      iata: "NME",
      name: "Nightmute Airport",
      city: "Nightmute, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5009,
      role: :spoke
    },
    "BET" => %{
      iata: "BET",
      name: "Bethel Airport",
      city: "Bethel, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5010,
      role: :spoke
    },
    "AKN" => %{
      iata: "AKN",
      name: "King Salmon Airport",
      city: "King Salmon, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5011,
      role: :spoke
    },
    "DLG" => %{
      iata: "DLG",
      name: "Dillingham Airport",
      city: "Dillingham, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5012,
      role: :spoke
    },
    "BRW" => %{
      iata: "BRW",
      name: "Wiley Post-Will Rogers Memorial Airport",
      city: "Utqiagvik, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5013,
      role: :spoke
    },
    "SCC" => %{
      iata: "SCC",
      name: "Deadhorse Airport",
      city: "Prudhoe Bay, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5014,
      role: :spoke
    },
    "CDV" => %{
      iata: "CDV",
      name: "Merle K. (Mudhole) Smith Airport",
      city: "Cordova, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5015,
      role: :spoke
    },
    "DUT" => %{
      iata: "DUT",
      name: "Unalaska Airport",
      city: "Dutch Harbor, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5016,
      role: :spoke
    },
    "KTN" => %{
      iata: "KTN",
      name: "Ketchikan International Airport",
      city: "Ketchikan, AK",
      ip: {127, 0, 0, 1},
      ip_str: "127.0.0.1",
      port: 5017,
      role: :spoke
    }
  }

  @ordered_codes ~w(ANC SEA FAI JNU SIT ADK ADQ OTZ NME BET AKN DLG BRW SCC CDV DUT KTN)

  @doc "Returns the default simulated airline name."
  @spec default_airline() :: String.t()
  def default_airline, do: @default_airline

  @doc "Returns all 17 airport descriptors sorted by port number."
  @spec all_airports() :: [airport_info()]
  def all_airports do
    Enum.map(@ordered_codes, &Map.fetch!(@airports, &1))
  end

  @doc "Returns the ordered list of all 17 IATA codes."
  @spec all_codes() :: [String.t()]
  def all_codes, do: @ordered_codes

  @doc "Returns all hub airport descriptors (ANC and SEA)."
  @spec hubs() :: [airport_info()]
  def hubs do
    Enum.filter(all_airports(), &(&1.role == :hub))
  end

  @doc "Returns all 15 spoke airport descriptors."
  @spec spokes() :: [airport_info()]
  def spokes do
    Enum.filter(all_airports(), &(&1.role == :spoke))
  end

  @doc "Normalizes an IATA code string, converting blank strings to nil."
  @spec normalize_code(String.t() | nil) :: String.t() | nil
  def normalize_code(nil), do: nil

  def normalize_code(code) when is_binary(code) do
    trimmed = code |> String.trim() |> String.upcase()
    if trimmed == "" or trimmed == "NONE" or trimmed == "DIRECT", do: nil, else: trimmed
  end

  @doc "Looks up an airport descriptor by IATA code."
  @spec get_airport(String.t() | nil) :: {:ok, airport_info()} | {:error, :unknown_airport}
  def get_airport(code) do
    case normalize_code(code) do
      nil ->
        {:error, :unknown_airport}

      iata ->
        case Map.fetch(@airports, iata) do
          {:ok, info} -> {:ok, info}
          :error -> {:error, :unknown_airport}
        end
    end
  end

  @doc "Returns true if the IATA code is one of the 17 supported airports."
  @spec valid_airport?(String.t() | nil) :: boolean()
  def valid_airport?(code) do
    match?({:ok, _}, get_airport(code))
  end

  @doc "Returns true if the airport is one of the two hubs (ANC or SEA)."
  @spec hub?(String.t() | nil) :: boolean()
  def hub?(code) do
    case get_airport(code) do
      {:ok, %{role: :hub}} -> true
      _ -> false
    end
  end

  @doc "Returns true if the airport is one of the 15 spoke airports."
  @spec spoke?(String.t() | nil) :: boolean()
  def spoke?(code) do
    case get_airport(code) do
      {:ok, %{role: :spoke}} -> true
      _ -> false
    end
  end

  @doc "Returns the formatted IP:Port string for an IATA code (e.g., \"127.0.0.1:5001\")."
  @spec address_for(String.t()) :: String.t()
  def address_for(code) do
    case get_airport(code) do
      {:ok, info} -> "#{info.ip_str}:#{info.port}"
      {:error, :unknown_airport} -> "unknown"
    end
  end

  @doc """
  Validates a flight route against the Alaska Airlines hub-and-spoke rules:
  - Carrier must be Alaska Airlines (or AS).
  - Origin, destination, and optional layover must be within the 17 allowed airports.
  - Origin and destination must differ.
  - Only ANC and SEA may serve as layover hubs; layover cannot equal origin or destination.
  - Direct flights between two spoke airports (without a hub layover) are prohibited.
  """
  @spec validate_route(String.t(), String.t(), String.t() | nil, String.t()) ::
          :ok | {:error, atom(), String.t()}
  def validate_route(from, to, layover \\ nil, airline \\ @default_airline) do
    origin = normalize_code(from)
    dest = normalize_code(to)
    via = normalize_code(layover)
    carrier = if is_binary(airline), do: String.trim(airline), else: ""

    cond do
      carrier not in @allowed_airlines ->
        {:error, :unsupported_airline,
         "Airline #{inspect(airline)} is outside simulation scope; only Alaska Airlines traffic is permitted."}

      not valid_airport?(origin) ->
        {:error, :unknown_airport,
         "Origin airport #{inspect(from)} is outside the supported Alaska and Seattle network."}

      not valid_airport?(dest) ->
        {:error, :unknown_airport,
         "Destination airport #{inspect(to)} is outside the supported Alaska and Seattle network."}

      origin == dest ->
        {:error, :same_origin_and_destination,
         "Origin and destination airports cannot be identical (#{origin})."}

      via != nil and not valid_airport?(via) ->
        {:error, :unknown_airport,
         "Layover airport #{inspect(layover)} is outside the supported Alaska and Seattle network."}

      via != nil and not hub?(via) ->
        {:error, :invalid_layover_not_a_hub,
         "Airport #{via} cannot serve as a transit point; only hubs ANC and SEA permit layovers."}

      via != nil and (via == origin or via == dest) ->
        {:error, :redundant_layover,
         "Layover hub #{via} cannot be the same as origin (#{origin}) or destination (#{dest})."}

      via == nil and spoke?(origin) and spoke?(dest) ->
        {:error, :direct_spoke_to_spoke_prohibited,
         "Direct flight between spoke airports #{origin} and #{dest} is prohibited; a layover at ANC or SEA is required."}

      true ->
        :ok
    end
  end

  @doc """
  Resolves an automatic layover hub (`ANC`) when `layover == :auto` and both endpoints
  are spokes, or validates the explicitly provided layover.
  """
  @spec resolve_route(String.t(), String.t(), String.t() | :auto | nil, String.t()) ::
          {:ok, String.t() | nil} | {:error, atom(), String.t()}
  def resolve_route(from, to, :auto, airline) do
    origin = normalize_code(from)
    dest = normalize_code(to)

    auto_layover =
      if valid_airport?(origin) and valid_airport?(dest) and origin != dest and
           spoke?(origin) and spoke?(dest) do
        "ANC"
      else
        nil
      end

    resolve_route(origin, dest, auto_layover, airline)
  end

  def resolve_route(from, to, layover, airline) do
    case validate_route(from, to, layover, airline) do
      :ok -> {:ok, normalize_code(layover)}
      error -> error
    end
  end
end
