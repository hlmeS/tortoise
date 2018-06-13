defmodule Tortoise.Package.Pingresp do
  @moduledoc false

  @opcode 13

  alias Tortoise.Package

  @opaque t :: %__MODULE__{
            __META__: Package.Meta.t()
          }
  defstruct __META__: %Package.Meta{opcode: @opcode, flags: 0}

  @spec decode(binary()) :: t
  def decode(<<@opcode::4, 0::4, 0>>) do
    %__MODULE__{}
  end

  # Protocols ----------------------------------------------------------
  defimpl Tortoise.Encodable do
    def encode(%Package.Pingresp{} = t) do
      [Package.Meta.encode(t.__META__), 0]
    end
  end
end
