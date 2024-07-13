open Array_to_array
open Bytes_to_bytes

module Ndarray = Owl.Dense.Ndarray.Generic

type endianness = Little | Big

type loc = Start | End

type arraytobytes =
  [ `Bytes of endianness
  | `ShardingIndexed of shard_config ]

and shard_config =
  {chunk_shape : int array
  ;codecs : bytestobytes shard_chain
  ;index_codecs : fixed_bytestobytes shard_chain
  ;index_location : loc}

and 'a shard_chain =
  {a2a: arraytoarray list
  ;a2b: arraytobytes
  ;b2b: 'a list}

type error =
  [ Extensions.error
  | Array_to_array.error
  | Bytes_to_bytes.error
  | `Sharding of int array * int array * string ]

module ArrayToBytes : sig
  val parse
    : ('a, 'b) Util.array_repr ->
      arraytobytes ->
      (unit, [> error]) result
  val compute_encoded_size : int -> arraytobytes -> int
  val default : arraytobytes
  val encode
    : ('a, 'b) Ndarray.t ->
      arraytobytes ->
      (string, [> error]) result
  val decode
    : string ->
      ('a, 'b) Util.array_repr ->
      arraytobytes ->
      (('a, 'b) Ndarray.t, [> error]) result
  val of_yojson : Yojson.Safe.t -> (arraytobytes, string) result
  val to_yojson : arraytobytes -> Yojson.Safe.t
end
