defmodule Archiviste.Record do
  @moduledoc """
  A single WARC record yielded by `Archiviste.stream!/2`.

  The `:payload` field is a lazy `Stream.t()` of binary chunks. It is
  **forward-only and single-pass** — reading consumes it. When the outer
  record stream advances to the next record, any unconsumed payload bytes
  are auto-drained.
  """

  @type warc_type ::
          :warcinfo
          | :response
          | :request
          | :metadata
          | :resource
          | :revisit
          | :conversion
          | :continuation
          | binary()

  @type t :: %__MODULE__{
          version: String.t(),
          type: warc_type(),
          id: String.t(),
          date: DateTime.t(),
          target_uri: String.t() | nil,
          content_type: String.t() | nil,
          content_length: non_neg_integer(),
          headers: %{optional(String.t()) => String.t()},
          payload: Enumerable.t(),
          offset: non_neg_integer()
        }

  @enforce_keys [
    :version,
    :type,
    :id,
    :date,
    :content_length,
    :headers,
    :payload,
    :offset
  ]
  defstruct [
    :version,
    :type,
    :id,
    :date,
    :target_uri,
    :content_type,
    :content_length,
    :headers,
    :payload,
    :offset
  ]

  @doc """
  Reads the full payload into memory as a binary.

  Convenient for small payloads. Do not use on records whose
  `:content_length` may be large.
  """
  @spec read_payload(t()) :: binary()
  def read_payload(%__MODULE__{payload: payload}) do
    payload |> Enum.to_list() |> IO.iodata_to_binary()
  end

  @doc """
  Drains and discards the payload. Returns `:ok`.
  """
  @spec discard_payload(t()) :: :ok
  def discard_payload(%__MODULE__{payload: payload}) do
    Stream.run(payload)
  end
end
