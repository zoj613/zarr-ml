module Ndarray = Owl.Dense.Ndarray.Generic

type dimension_order = int array

type array_to_array =
  | Transpose of dimension_order

type compression_level =
  | L0 | L1 | L2 | L3 | L4 | L5 | L6 | L7 | L8 | L9

type bytes_to_bytes =
  | Crc32c
  | Gzip of compression_level

type endianness = Little | Big

type loc = Start | End

type array_to_bytes =
  | Bytes of endianness
  | ShardingIndexed of shard_config

and shard_config =
  {chunk_shape : int array
  ;codecs : chain
  ;index_codecs : chain
  ;index_location : loc}

and chain = {
  a2a: array_to_array list;
  a2b: array_to_bytes;
  b2b: bytes_to_bytes list;
}

type error = Array_to_bytes.error

module Chain : sig
  type t

  val create
    : ('a, 'b) Util.array_repr -> chain -> (t, [> error]) result

  val default : t

  val compute_encoded_size : int -> t -> int

  val encode
    : t -> ('a, 'b) Ndarray.t -> (string, [> error]) result

  val decode
    : t ->
      ('a, 'b) Util.array_repr ->
      string ->
      (('a, 'b) Ndarray.t, [> error]) result

  val equal : t -> t -> bool

  val of_yojson : Yojson.Safe.t -> (t, string) result

  val to_yojson : t -> Yojson.Safe.t

  val pp : Format.formatter -> t -> unit

  val show : t -> string
end
