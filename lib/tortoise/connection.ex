defmodule Tortoise.Connection do
  @moduledoc """
  Establish a connection to a MQTT broker.

  Todo.
  """

  use GenServer

  require Logger

  defstruct [:connect, :server, :session, :subscriptions, :keep_alive]
  alias __MODULE__, as: State

  @type client_id() :: binary() | atom()

  alias Tortoise.{Transport, Connection, Package}
  alias Tortoise.Connection.{Inflight, Controller, Receiver, Transmitter}
  alias Tortoise.Package.{Connect, Connack}

  def start_link(opts) do
    client_id = Keyword.fetch!(opts, :client_id)
    server = opts |> Keyword.fetch!(:server) |> Transport.new()

    connect = %Package.Connect{
      client_id: client_id,
      user_name: Keyword.get(opts, :user_name),
      password: Keyword.get(opts, :password),
      keep_alive: Keyword.get(opts, :keep_alive, 60),
      will: Keyword.get(opts, :last_will),
      # if we re-spawn from here it means our state is gone
      clean_session: true
    }

    subscriptions =
      case Keyword.get(opts, :subscriptions, []) do
        topics when is_list(topics) ->
          Enum.into(topics, %Package.Subscribe{})

        %Package.Subscribe{} = subscribe ->
          subscribe
      end

    # @todo, validate that the handler is valid
    opts = Keyword.take(opts, [:client_id, :handler])
    initial = {server, connect, subscriptions, opts}
    GenServer.start_link(__MODULE__, initial, name: via_name(client_id))
  end

  defp via_name(client_id) do
    Tortoise.Registry.via_name(__MODULE__, client_id)
  end

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @doc false
  def publish(client_id, topic, payload \\ nil, opts \\ []) do
    qos = Keyword.get(opts, :qos, 0)

    publish = %Package.Publish{
      topic: topic,
      qos: qos,
      payload: payload,
      retain: Keyword.get(opts, :retain, false)
    }

    case publish do
      %Package.Publish{qos: 0} ->
        Transmitter.cast(client_id, publish)

      %Package.Publish{qos: qos} when qos in [1, 2] ->
        Inflight.track(client_id, {:outgoing, publish})
    end
  end

  @doc false
  def publish_sync(client_id, topic, payload \\ nil, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, :infinity)
    qos = Keyword.get(opts, :qos, 0)

    publish = %Package.Publish{
      topic: topic,
      qos: qos,
      payload: payload,
      retain: Keyword.get(opts, :retain, false)
    }

    case publish do
      %Package.Publish{qos: 0} ->
        Transmitter.cast(client_id, publish)

      %Package.Publish{qos: qos} when qos in [1, 2] ->
        Inflight.track_sync(client_id, {:outgoing, publish}, timeout)
    end
  end

  @doc false
  def subscribe(client_id, topics, opts \\ [])

  def subscribe(client_id, [{_, n} | _] = topics, opts) when is_number(n) do
    caller = {_, ref} = {self(), make_ref()}
    {identifier, opts} = Keyword.pop_first(opts, :identifier, nil)
    subscribe = Enum.into(topics, %Package.Subscribe{identifier: identifier})
    GenServer.cast(via_name(client_id), {:subscribe, caller, subscribe, opts})
    {:ok, ref}
  end

  def subscribe(client_id, {_, n} = topic, opts) when is_number(n) do
    subscribe(client_id, [topic], opts)
  end

  def subscribe(client_id, topic, opts) when is_binary(topic) do
    case Keyword.pop_first(opts, :qos) do
      {nil, _opts} ->
        throw("Please specify a quality of service for the subscription")

      {qos, opts} when qos in 0..2 ->
        subscribe(client_id, [{topic, qos}], opts)
    end
  end

  @doc false
  def subscribe_sync(client_id, topics, opts \\ [])

  def subscribe_sync(client_id, [{_, n} | _] = topics, opts) when is_number(n) do
    timeout = Keyword.get(opts, :timeout, 5000)
    {:ok, ref} = subscribe(client_id, topics, opts)

    receive do
      {{Tortoise, ^client_id}, ^ref, result} -> result
    after
      timeout ->
        {:error, :timeout}
    end
  end

  def subscribe_sync(client_id, {_, n} = topic, opts) when is_number(n) do
    subscribe_sync(client_id, [topic], opts)
  end

  def subscribe_sync(client_id, topic, opts) when is_binary(topic) do
    case Keyword.pop_first(opts, :qos) do
      {nil, _opts} ->
        throw("Please specify a quality of service for the subscription")

      {qos, opts} ->
        subscribe_sync(client_id, [{topic, qos}], opts)
    end
  end

  @doc false
  def unsubscribe(client_id, topics, opts \\ [])

  def unsubscribe(client_id, [topic | _] = topics, opts) when is_binary(topic) do
    caller = {_, ref} = {self(), make_ref()}
    {identifier, opts} = Keyword.pop_first(opts, :identifier, nil)
    unsubscribe = %Package.Unsubscribe{identifier: identifier, topics: topics}
    GenServer.cast(via_name(client_id), {:unsubscribe, caller, unsubscribe, opts})
    {:ok, ref}
  end

  def unsubscribe(client_id, topic, opts) when is_binary(topic) do
    unsubscribe(client_id, [topic], opts)
  end

  @doc false
  def unsubscribe_sync(client_id, topics, opts \\ [])

  def unsubscribe_sync(client_id, topics, opts) when is_list(topics) do
    timeout = Keyword.get(opts, :timeout, 5000)
    {:ok, ref} = unsubscribe(client_id, topics, opts)

    receive do
      {{Tortoise, ^client_id}, ^ref, result} -> result
    after
      timeout ->
        {:error, :timeout}
    end
  end

  def unsubscribe_sync(client_id, topic, opts) when is_binary(topic) do
    unsubscribe_sync(client_id, [topic], opts)
  end

  def subscriptions(client_id) do
    GenServer.call(via_name(client_id), :subscriptions)
  end

  @doc false
  @spec renew(client_id()) :: :ok
  def renew(client_id) do
    GenServer.cast(via_name(client_id), :renew_connection)
  end

  # Callbacks
  def init({transport, %Connect{} = connect, subscriptions, opts}) do
    expected_connack = %Connack{status: :accepted, session_present: false}

    with {^expected_connack, socket} <- do_connect(transport, connect),
         {:ok, pid} = Connection.Supervisor.start_link(opts),
         :ok = Receiver.handle_socket(connect.client_id, {transport.type, socket}),
         :ok = Controller.update_connection_status(connect.client_id, :up) do
      if not Enum.empty?(subscriptions), do: send(self(), :subscribe)

      result = %State{
        session: pid,
        server: transport,
        connect: connect,
        subscriptions: subscriptions
      }

      {:ok, reset_keep_alive(result)}
    else
      %Connack{status: {:refused, reason}} ->
        {:stop, {:connection_failed, reason}}

      {:error, {:protocol_violation, violation}} ->
        Logger.error("Protocol violation: #{inspect(violation)}")
        {:stop, :protocol_violation}
    end
  end

  def handle_info(:subscribe, %State{subscriptions: subscriptions} = state) do
    client_id = state.connect.client_id

    case Enum.empty?(subscriptions) do
      true ->
        # nothing to subscribe to, just continue
        {:noreply, state}

      false ->
        # subscribe to the predefined topics
        case Inflight.track_sync(client_id, {:outgoing, subscriptions}, 5000) do
          {:error, :timeout} ->
            {:stop, :subscription_timeout, state}

          result ->
            case handle_suback_result(result, state) do
              {:ok, updated_state} ->
                {:noreply, updated_state}

              {:error, reasons} ->
                error = {:unable_to_subscribe, reasons}
                {:stop, error, state}
            end
        end
    end
  end

  def handle_info(:ping, %State{} = state) do
    case Controller.ping_sync(state.connect.client_id, 5000) do
      {:ok, round_trip_time} ->
        Logger.debug("Ping: #{round_trip_time} μs")
        state = reset_keep_alive(state)
        {:noreply, state}

      {:error, :timeout} ->
        {:stop, :ping_timeout, state}
    end
  end

  # dropping connection
  def handle_info({transport, _socket}, state) when transport in [:tcp_closed, :ssl_closed] do
    Logger.error("Socket closed before we handed it to the receiver")
    :ok = Controller.update_connection_status(state.connect.client_id, :down)
    do_attempt_reconnect(state)
  end

  def handle_call(:subscriptions, _from, state) do
    {:reply, state.subscriptions, state}
  end

  def handle_cast(:renew_connection, state) do
    :ok = Controller.update_connection_status(state.connect.client_id, :down)
    do_attempt_reconnect(state)
  end

  def handle_cast({:subscribe, {caller_pid, ref}, subscribe, opts}, state) do
    client_id = state.connect.client_id
    timeout = Keyword.get(opts, :timeout, 5000)

    case Inflight.track_sync(client_id, {:outgoing, subscribe}, timeout) do
      {:error, :timeout} = error ->
        send(caller_pid, {{Tortoise, client_id}, ref, error})
        {:noreply, state}

      result ->
        case handle_suback_result(result, state) do
          {:ok, updated_state} ->
            send(caller_pid, {{Tortoise, client_id}, ref, :ok})
            {:noreply, updated_state}

          {:error, reasons} ->
            error = {:unable_to_subscribe, reasons}
            send(caller_pid, {{Tortoise, client_id}, ref, {:error, reasons}})
            {:stop, error, state}
        end
    end
  end

  def handle_cast({:unsubscribe, {caller_pid, ref}, unsubscribe, opts}, state) do
    client_id = state.connect.client_id
    timeout = Keyword.get(opts, :timeout, 5000)

    case Inflight.track_sync(client_id, {:outgoing, unsubscribe}, timeout) do
      {:error, :timeout} = error ->
        send(caller_pid, {{Tortoise, client_id}, ref, error})
        {:noreply, state}

      unsubbed ->
        topics = Keyword.drop(state.subscriptions.topics, unsubbed)
        subscriptions = %Package.Subscribe{state.subscriptions | topics: topics}
        send(caller_pid, {{Tortoise, client_id}, ref, :ok})
        {:noreply, %State{state | subscriptions: subscriptions}}
    end
  end

  # Helpers
  defp handle_suback_result(%{:error => []} = results, %State{} = state) do
    subscriptions = Enum.into(results[:ok], state.subscriptions)
    {:ok, %State{state | subscriptions: subscriptions}}
  end

  defp handle_suback_result(%{:error => errors}, %State{}) do
    {:error, errors}
  end

  defp reset_keep_alive(%State{keep_alive: nil} = state) do
    ref = Process.send_after(self(), :ping, state.connect.keep_alive * 1000)
    %State{state | keep_alive: ref}
  end

  defp reset_keep_alive(%State{keep_alive: previous_ref} = state) do
    # Cancel the previous timer, just in case one was already set
    _ = Process.cancel_timer(previous_ref)
    ref = Process.send_after(self(), :ping, state.connect.keep_alive * 1000)
    %State{state | keep_alive: ref}
  end

  defp do_connect(server, %Connect{} = connect) do
    %Transport{type: transport, host: host, port: port, opts: opts} = server

    with {:ok, socket} <- transport.connect(host, port, opts, 10000),
         :ok = transport.send(socket, Package.encode(connect)),
         {:ok, packet} <- transport.recv(socket, 4, 5000) do
      case Package.decode(packet) do
        %Connack{status: :accepted} = connack ->
          {connack, socket}

        %Connack{status: {:refused, _reason}} = connack ->
          connack

        other ->
          violation = %{expected: Connect, got: other}
          {:error, {:protocol_violation, violation}}
      end
    else
      {:error, :econnrefused} ->
        {:error, {:connection_refused, host, port}}
    end
  end

  defp do_attempt_reconnect(%State{server: transport} = state) do
    connect = %Connect{state.connect | clean_session: false}

    with {%Connack{status: :accepted} = connack, socket} <- do_connect(transport, connect),
         :ok = Receiver.handle_socket(connect.client_id, {transport.type, socket}),
         :ok = Controller.update_connection_status(connect.client_id, :up) do
      case connack do
        %Connack{session_present: true} ->
          result = %State{state | connect: connect}
          {:noreply, reset_keep_alive(result)}

        %Connack{session_present: false} ->
          # delete inflight state ?
          if not Enum.empty?(state.subscriptions), do: send(self(), :subscribe)
          result = %State{state | connect: connect}
          {:noreply, reset_keep_alive(result)}
      end
    else
      %Connack{status: {:refused, reason}} ->
        {:stop, {:connection_failed, reason}}

      {:error, {:protocol_violation, violation}} ->
        Logger.error("Protocol violation: #{inspect(violation)}")
        {:stop, :protocol_violation}
    end
  end
end
