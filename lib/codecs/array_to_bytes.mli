open Array_to_array
open Bytes_to_bytes

module Ndarray = Owl.Dense.Ndarray.Generic

type endianness = Little | Big

type loc = Start | End

type array_to_bytes =
  | Bytes of endianness
  | ShardingIndexed of shard_config

and shard_config =
  {chunk_shape : int array
  ;codecs : any_bytes_to_bytes sharding_chain
  ;index_codecs : fixed bytes_to_bytes sharding_chain
  ;index_location : loc}

and 'a sharding_chain = {
  a2a: array_to_array list;
  a2b: array_to_bytes;
  b2b: 'a list}

type error =
  [ Extensions.error
  | Array_to_array.error
  | Bytes_to_bytes.error
  | `Sharding of int array * int array * string ]

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
