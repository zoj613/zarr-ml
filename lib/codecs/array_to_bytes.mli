module Ndarray = Owl.Dense.Ndarray.Generic

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
  a2a: Array_to_array.array_to_array list;
  a2b: array_to_bytes;
  b2b: Bytes_to_bytes.bytes_to_bytes list;
}

val pp_chain : Format.formatter -> chain -> unit
val show_chain : chain -> string

type error =
  [ `Bytes_encode_error of string
  | `Bytes_decode_error of string
  | `Sharding_shape_mismatch of int array * int array
  | Array_to_array.error
  | Bytes_to_bytes.error ]

module ArrayToBytes : sig
  val parse
    : ('a, 'b) Util.array_repr ->
      array_to_bytes ->
      (unit, [> error]) result
  val compute_encoded_size : int -> array_to_bytes -> int
  val default : array_to_bytes
  val encode
    : ('a, 'b) Ndarray.t ->
      array_to_bytes ->
      (string, [> error]) result
  val decode
    : string ->
      ('a, 'b) Util.array_repr ->
      array_to_bytes ->
      (('a, 'b) Ndarray.t, [> error]) result
  val of_yojson : Yojson.Safe.t -> (array_to_bytes, string) result
  val to_yojson : array_to_bytes -> Yojson.Safe.t
end
