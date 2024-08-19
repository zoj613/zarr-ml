exception Bytes_to_bytes_invariant
exception Invalid_transpose_order
exception Invalid_sharding_chunk_shape

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

type endianness = LE | BE

type loc = Start | End

type fixed_arraytobytes =
  [ `Bytes of endianness ]

type variable_arraytobytes =
  [ `ShardingIndexed of internal_shard_config ]

and internal_shard_config =
  {chunk_shape : int array
  ;codecs :
    ([fixed_arraytobytes | `ShardingIndexed of internal_shard_config ]
    ,bytestobytes) internal_chain
  ;index_codecs : (fixed_arraytobytes, fixed_bytestobytes) internal_chain

  ;index_location : loc}

and ('a, 'b) internal_chain =
  {a2a : arraytoarray list
  ;a2b : 'a
  ;b2b : 'b list}

type arraytobytes = [ fixed_arraytobytes | variable_arraytobytes ]

type ('a, 'b) array_repr =
  {kind : ('a, 'b) Bigarray.kind
  ;shape : int array}

module type Interface = sig
  exception Bytes_to_bytes_invariant
  (** raised when a codec chain contains more than 1 bytes->bytes codec. *)

  exception Invalid_transpose_order
  (** raised when a codec chain contains a Transpose codec with an incorrect order. *)

  exception Invalid_sharding_chunk_shape
  (** raise when a codec chain contains a shardingindexed codec with an incorrect inner chunk shape. *)

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
  type endianness = LE | BE

  (** A type representing the location of a shard's index array in
      an encoded byte string. *)
  type loc = Start | End

  (** The type of [array -> bytes] codecs that produce
      fixed sized encoded string. *)
  type fixed_arraytobytes =
    [ `Bytes of endianness ]

  (** The type of [array -> bytes] codecs that produce
      variable sized encoded string. *)
  type variable_array_tobytes =
    [ `ShardingIndexed of shard_config ]

  (** A type representing the Sharding indexed codec's configuration parameters. *)
  and shard_config =
    {chunk_shape : int array
    ;codecs :
      [ arraytoarray
      | fixed_arraytobytes
      | `ShardingIndexed of shard_config
      | bytestobytes ] list
    ;index_codecs :
      [ arraytoarray | fixed_arraytobytes | fixed_bytestobytes ] list
    ;index_location : loc}

  (** The type of [array -> bytes] codecs. *)
  type array_tobytes =
    [ fixed_arraytobytes | variable_array_tobytes ]

  (** A type used to build a user-defined chain of codecs when creating a Zarr array. *)
  type codec_chain =
    [ arraytoarray | array_tobytes | bytestobytes ] list

  (** The type summarizing the decoded/encoded representation of a Zarr array
      or chunk. *)
  type ('a, 'b) array_repr =
    {kind : ('a, 'b) Bigarray.kind
    ;shape : int array}
end
