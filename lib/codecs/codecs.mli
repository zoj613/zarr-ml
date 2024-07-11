module Ndarray = Owl.Dense.Ndarray.Generic

type arraytoarray =
  [ `Transpose of int array ]

type compression_level =
  | L0 | L1 | L2 | L3 | L4 | L5 | L6 | L7 | L8 | L9

type fixed_bytestobytes =
  [ `Crc32c ]
type variable_bytestobytes =
  [ `Gzip of compression_level ]
type bytestobytes =
  [ fixed_bytestobytes | variable_bytestobytes ]

type endianness = Little | Big

type loc = Start | End

type arraytobytes =
  [ `Bytes of endianness
  | `ShardingIndexed of sharding_config ]

and sharding_config =
  {chunk_shape : int array
  ;codecs : bytestobytes shard_chain
  ;index_codecs : fixed_bytestobytes shard_chain
  ;index_location : loc}

and 'a shard_chain = {
  a2a: arraytoarray list;
  a2b: arraytobytes;
  b2b: 'a list;
}

type codec_chain = {
  a2a: arraytoarray list;
  a2b: arraytobytes;
  b2b: bytestobytes list;
}

type error = Array_to_bytes.error

module Chain : sig
  type t

  val create
    : ('a, 'b) Util.array_repr -> codec_chain -> (t, [> error]) result

  val default : t

  val compute_encoded_size : int -> t -> int

  val encode
    : t -> ('a, 'b) Ndarray.t -> (string, [> error]) result

  val decode
    : t ->
      ('a, 'b) Util.array_repr ->
      string ->
      (('a, 'b) Ndarray.t, [> error]) result

  val ( = ) : t -> t -> bool

  val of_yojson : Yojson.Safe.t -> (t, string) result

  val to_yojson : t -> Yojson.Safe.t

  val pp : Format.formatter -> t -> unit

  val show : t -> string
end
