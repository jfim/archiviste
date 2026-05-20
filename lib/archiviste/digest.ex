defmodule Archiviste.Digest do
  @moduledoc false

  @algos %{
    "sha1" => :sha,
    "sha256" => :sha256,
    "sha512" => :sha512,
    "md5" => :md5
  }

  # Supported in verification — exclude md5 by default since the WARC spec
  # treats it as legacy. Keep it parseable but not verifiable.
  @verified_algos ["sha1", "sha256", "sha512"]

  @spec verify(String.t(), iodata()) ::
          :ok
          | {:error, :mismatch, String.t(), String.t()}
          | {:error, :unknown_algorithm, String.t()}
  def verify(header, payload) when is_binary(header) do
    with {:ok, algo_str, expected_encoded} <- parse_header(header),
         true <- algo_str in @verified_algos or {:error, :unknown_algorithm, algo_str} do
      erlang_algo = Map.fetch!(@algos, algo_str)
      actual_bytes = :crypto.hash(erlang_algo, payload)
      expected_bytes = decode_digest(expected_encoded)

      if actual_bytes == expected_bytes do
        :ok
      else
        actual_encoded = Base.encode32(actual_bytes, padding: false)
        {:error, :mismatch, header, "#{algo_str}:#{actual_encoded}"}
      end
    else
      {:error, _, _} = err -> err
    end
  end

  defp parse_header(header) do
    case String.split(header, ":", parts: 2) do
      [algo, value] -> {:ok, String.downcase(algo), value}
      _ -> {:error, :unknown_algorithm, header}
    end
  end

  defp decode_digest(value) do
    if hex?(value) do
      Base.decode16!(value, case: :mixed)
    else
      Base.decode32!(value, padding: false)
    end
  end

  defp hex?(value), do: String.match?(value, ~r/^[0-9a-fA-F]+$/) and rem(byte_size(value), 2) == 0

  ## Streaming API

  @spec init(:sha | :sha256 | :sha512 | :md5) :: term()
  def init(algo), do: :crypto.hash_init(algo)

  @spec update(term(), iodata()) :: term()
  def update(state, data), do: :crypto.hash_update(state, data)

  @spec finalize_base32(term()) :: String.t()
  def finalize_base32(state),
    do: state |> :crypto.hash_final() |> Base.encode32(padding: false)

  @spec algo_from_header(String.t()) ::
          {:ok, atom(), String.t()} | {:error, :unknown_algorithm, String.t()}
  def algo_from_header(header) when is_binary(header) do
    with {:ok, algo_str, expected} <- parse_header(header),
         true <- algo_str in @verified_algos or {:error, :unknown_algorithm, algo_str} do
      {:ok, Map.fetch!(@algos, algo_str), expected}
    else
      {:error, _, _} = err -> err
    end
  end
end
