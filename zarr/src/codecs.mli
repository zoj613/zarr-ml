(** An array has an associated list of codecs. Each codec specifies a
    bidirectional transform (an encode transform and a decode transform).
    This module contains building blocks for creating and working with
    a chain of codecs. *)

type error =
  [ `Array_to_bytes_invariant  (** when a codec chain contains more than 1 array->bytes codec. *)
  | `Invalid_transpose_order  (** when a codec chain contains a Transpose codec with an incorrect order. *)
  | `Invalid_sharding_chunk_shape  (** when a codec chain contains a shardingindexed codec with an incorrect inner chunk shape. *)
  | `Invalid_codec_ordering  (** when a codec chain has incorrect ordering of codecs. i.e if the ordering is not [arraytoarray list -> 1 arraytobytes -> bytestobytes list]. *)
  | `Invalid_zstd_compression_level  (** when a codec chain contains a Zstd codec with an incorrect compression value.*)]

(** The type of [array -> array] codecs. *)
type arraytoarray = [ `Transpose of int list ]

(** A type representing valid compression levels of the DEFLATE algorithm. *)
type deflate_level = L0 | L1 | L2 | L3 | L4 | L5 | L6 | L7 | L8 | L9

(** A type representing [bytes -> bytes] codecs that produce
    fixed sized encoded strings. *)
type fixed_bytestobytes = [ `Crc32c ]

(** A type representing [bytes -> bytes] codecs that produce
    variable sized encoded strings. *)
type variable_bytestobytes = [ `Gzip of deflate_level | `Zstd of int * bool ]

(** The type of [bytes -> bytes] codecs. *)
type bytestobytes = [ fixed_bytestobytes | variable_bytestobytes ]

(** A type representing the configured endianness of an array. *)
type endianness = LE | BE

(** A type representing the location of a shard's index array in
    an encoded byte string. *)
type loc = Start | End

(** The type of [array -> bytes] codecs that produce
    fixed sized encoded string. *)
type fixed_arraytobytes = [ `Bytes of endianness ]

(** The type of [array -> bytes] codecs that produce
    variable sized encoded string. *)
type variable_array_tobytes = [ `ShardingIndexed of shard_config ]
and codec = [ arraytoarray | fixed_arraytobytes | `ShardingIndexed of shard_config | bytestobytes ]
and index_codec = [ arraytoarray | fixed_arraytobytes | fixed_bytestobytes ]

(** A type representing the Sharding indexed codec's configuration parameters. *)
and shard_config =
  {chunk_shape : int list
  ;codecs : codec list
  ;index_codecs : index_codec list
  ;index_location : loc}

(** The type summarizing the decoded/encoded representation of a Zarr array
    or chunk. *)
type 'a array_info = {datatype : 'a Ndarray.dtype; shape : int list}

(** A module containing functions to encode/decode an array chunk using a
    predefined set of codecs. *)
module Chain : sig
  (** A type representing a valid chain of codecs for
      decoding/encoding a Zarr array chunk. *)
  type t

  (** [create s c] returns a type representing a chain of codecs defined by chain [c] and chunk shape [s]. *)
  val create : int list -> codec list -> (t, [> error]) result

  (** [encode t x] computes the encoded byte string representation of
      array chunk [x]. *)
  val encode : t -> 'a Ndarray.t -> string

  (** [decode t repr x] decodes the byte string [x] using codec chain [t]
      and decoded representation type [repr]. *)
  val decode : t -> 'a array_info -> string -> 'a Ndarray.t

  (** [x = y] returns true if chain [x] is equal to chain [y],
      and false otherwise. *)
  val ( = ) : t -> t -> bool

  (** [of_yojson x] returns a code chain of type {!t} from its json object
      representation. *)
  val of_yojson : int list -> Yojson.Safe.t -> (t, string) result

  (** [to_yojson x] returns a json object representation of codec chain [x]. *)
  val to_yojson : t -> Yojson.Safe.t
end

(** A functor for generating a Sharding Indexed codec that supports partial
    (en/de)coding via IO operations. *)
module Make (IO : Types.IO) (Store : Types.Store with type 'a io = 'a IO.t) : sig
  (** [is_just_sharding t] is [true] if the codec chain [t] contains only
      the [sharding_indexed] codec. *)
  val is_just_sharding : Chain.t -> bool

  val partial_encode :
    fill_value:'a ->
    Store.t ->
    Types.key ->  (* shard key *)
    Chain.t ->
    'a array_info ->
    (Types.chunk_coord * 'a) list ->
    (unit, [> `Zarr of Store.error ]) result IO.t

  val partial_decode :
    fill_value:'a ->
    Store.t ->
    Types.key ->  (* shard key *)
    Chain.t ->
    'a array_info ->
    (int * Types.chunk_coord) list ->
    ((int * 'a) list, [> `Zarr of Store.error ]) result IO.t
end
