defmodule Archiviste.Reader do
  @moduledoc false
  # Process-backed byte cursor over an Enumerable.t(binary()).
  #
  # Owns the enumerable's continuation function and serves serialized
  # read/skip/peek calls. Used internally by Archiviste.stream!/2; both
  # the outer record stream and the inner payload sub-stream call into
  # the same Reader pid.

  use GenServer

  @type t :: pid()

  ## Client

  def start_link(enumerable) do
    GenServer.start_link(__MODULE__, enumerable)
  end

  def read(pid, n) when is_integer(n) and n >= 0,
    do: GenServer.call(pid, {:read, n}, :infinity)

  def peek(pid, n) when is_integer(n) and n >= 0,
    do: GenServer.call(pid, {:peek, n}, :infinity)

  def skip(pid, n) when is_integer(n) and n >= 0,
    do: GenServer.call(pid, {:skip, n}, :infinity)

  def read_until(pid, delim) when is_binary(delim),
    do: GenServer.call(pid, {:read_until, delim}, :infinity)

  def offset(pid), do: GenServer.call(pid, :offset, :infinity)

  def eof?(pid), do: GenServer.call(pid, :eof?, :infinity)

  def close(pid), do: GenServer.stop(pid)

  ## Server

  defmodule State do
    @moduledoc false
    defstruct [:cont, :buffer, :offset, :eof]
  end

  @impl true
  def init(enumerable) do
    reducer = &Enumerable.reduce(enumerable, &1, fn x, _ -> {:suspend, x} end)
    state = %State{cont: reducer, buffer: <<>>, offset: 0, eof: false}
    {:ok, state}
  end

  @impl true
  def handle_call({:read, n}, _from, state) do
    case ensure_buffered(state, n) do
      {:ok, state} ->
        <<chunk::binary-size(n), rest::binary>> = state.buffer
        {:reply, {:ok, chunk}, %{state | buffer: rest, offset: state.offset + n}}

      {:eof, state} ->
        {:reply, :eof, %{state | buffer: <<>>}}
    end
  end

  def handle_call({:peek, n}, _from, state) do
    case ensure_buffered(state, n) do
      {:ok, state} ->
        <<chunk::binary-size(n), _::binary>> = state.buffer
        {:reply, {:ok, chunk}, state}

      {:eof, state} ->
        {:reply, :eof, state}
    end
  end

  def handle_call({:skip, n}, _from, state) do
    {result, state} = do_skip(state, n)
    {:reply, result, state}
  end

  def handle_call({:read_until, delim}, _from, state) do
    {result, state} = do_read_until(state, delim, byte_size(delim))
    {:reply, result, state}
  end

  def handle_call(:offset, _from, state),
    do: {:reply, state.offset, state}

  def handle_call(:eof?, _from, state),
    do: {:reply, state.eof and state.buffer == <<>>, state}

  ## Internals

  defp ensure_buffered(state, n) do
    cond do
      byte_size(state.buffer) >= n ->
        {:ok, state}

      state.eof ->
        if byte_size(state.buffer) >= n, do: {:ok, state}, else: {:eof, state}

      true ->
        case pull(state) do
          {:ok, state} -> ensure_buffered(state, n)
          {:eof, state} -> ensure_buffered(state, n)
        end
    end
  end

  defp pull(%State{cont: cont} = state) do
    case cont.({:cont, nil}) do
      {:suspended, chunk, next} when is_binary(chunk) ->
        {:ok, %{state | buffer: state.buffer <> chunk, cont: next}}

      {:suspended, iodata, next} ->
        {:ok,
         %{
           state
           | buffer: state.buffer <> IO.iodata_to_binary(iodata),
             cont: next
         }}

      {:done, _} ->
        {:eof, %{state | eof: true}}

      {:halted, _} ->
        {:eof, %{state | eof: true}}
    end
  end

  defp do_skip(state, 0), do: {:ok, state}

  defp do_skip(%State{buffer: buf, offset: off} = state, n) do
    case byte_size(buf) do
      size when size >= n ->
        <<_::binary-size(n), rest::binary>> = buf
        {:ok, %{state | buffer: rest, offset: off + n}}

      size ->
        do_skip_refill(%{state | buffer: <<>>, offset: off + size}, n - size)
    end
  end

  defp do_skip_refill(%State{eof: true} = state, _remaining), do: {:eof, state}

  defp do_skip_refill(state, remaining) do
    case pull(state) do
      {:ok, state} -> do_skip(state, remaining)
      {:eof, state} -> {:eof, state}
    end
  end

  defp do_read_until(state, delim, delim_size) do
    case :binary.match(state.buffer, delim) do
      {pos, ^delim_size} ->
        take = pos + delim_size
        <<chunk::binary-size(take), rest::binary>> = state.buffer
        {{:ok, chunk}, %{state | buffer: rest, offset: state.offset + take}}

      :nomatch ->
        do_read_until_refill(state, delim, delim_size)
    end
  end

  defp do_read_until_refill(%State{eof: true} = state, _delim, _delim_size), do: {:eof, state}

  defp do_read_until_refill(state, delim, delim_size) do
    case pull(state) do
      {:ok, state} -> do_read_until(state, delim, delim_size)
      {:eof, state} -> do_read_until(state, delim, delim_size)
    end
  end
end
