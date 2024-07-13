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

type codec_chain =
  [ arraytoarray | arraytobytes | bytestobytes ] list

type error =
  [ `Extension of string 
  | `Gzip of Ezgzip.error
  | `Transpose_order of int array * string
  | `CodecChain of string
  | `Sharding of int array * int array * string ]

module Ndarray = Owl.Dense.Ndarray.Generic

module type Interface = sig
  (** The type of [array -> array] codecs. *)
  type arraytoarray =
    [ `Transpose of int array ]

  (** A type representing valid Gzip codec compression levels. *)
  type compression_level =
    | L0 | L1 | L2 | L3 | L4 | L5 | L6 | L7 | L8 | L9

  (** A type representing [bytes -> bytes] codecs that produce
      fixed sized encoded strings. *)
  type fixed_bytestobytes =
    [ `Crc32c ]

  (** A type representing [bytes -> bytes] codecs that produce
      variable sized encoded strings. *)
  type variable_bytestobytes =
    [ `Gzip of compression_level ]

  (** The type of [bytes -> bytes] codecs. *)
  type bytestobytes =
    [ fixed_bytestobytes | variable_bytestobytes ]

  (** A type representing the configured endianness of an array. *)
  type endianness = Little | Big

  (** A type representing the location of a shard's index array in
      an encoded byte string. *)
  type loc = Start | End

  (** The type of [array -> bytes] codecs. *)
  type arraytobytes =
    [ `Bytes of endianness
    | `ShardingIndexed of shard_config ]

  (** A type representing the Sharding indexed codec's configuration parameters. *)
  and shard_config =
    {chunk_shape : int array
    ;codecs : bytestobytes shard_chain
    ;index_codecs : fixed_bytestobytes shard_chain
    ;index_location : loc}

  (** A type representing the chain of codecs used to encode/decode
      a shard's bytes and its index array. *)
  and 'a shard_chain =
    {a2a: arraytoarray list
    ;a2b: arraytobytes
    ;b2b: 'a list}

  (** A type used to build a user-defined chain of codecs when creating a Zarr array. *)
  type codec_chain =
    [ arraytoarray | arraytobytes | bytestobytes ] list

  (** The type of errors returned upon failure when an calling a function
    on a {!Chain} type. *)
  type error =
    [ `Extension of string 
    | `Gzip of Ezgzip.error
    | `Transpose_order of int array * string
    | `CodecChain of string
    | `Sharding of int array * int array * string ]
end
