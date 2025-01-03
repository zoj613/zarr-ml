(** An array has an associated list of codecs. Each codec specifies a
    bidirectional transform (an encode transform and a decode transform).
    This module contains building blocks for creating and working with
    a chain of codecs. *)

exception Array_to_bytes_invariant
(** raised when a codec chain contains more than 1 array->bytes codec. *)

exception Invalid_transpose_order
(** raised when a codec chain contains a Transpose codec with an incorrect order. *)

exception Invalid_sharding_chunk_shape
(** raise when a codec chain contains a shardingindexed codec with an incorrect inner chunk shape. *)

exception Invalid_codec_ordering
(** raised when a codec chain has incorrect ordering of codecs. i.e if the
    ordering is not [arraytoarray list -> 1 arraytobytes -> bytestobytes list]. *)

exception Invalid_zstd_level
(** raised when a codec chain contains a Zstd codec with an incorrect compression value.*)

(** The type of [array -> array] codecs. *)
type arraytoarray = [ `Transpose of int list ]

(** A type representing valid Gzip codec compression levels. *)
type compression_level = L0 | L1 | L2 | L3 | L4 | L5 | L6 | L7 | L8 | L9

(** A type representing [bytes -> bytes] codecs that produce
    fixed sized encoded strings. *)
type fixed_bytestobytes = [ `Crc32c ]

(** A type representing [bytes -> bytes] codecs that produce
    variable sized encoded strings. *)
type variable_bytestobytes = [ `Gzip of compression_level | `Zstd of int * bool ]

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
type 'a array_repr = {kind : 'a Ndarray.dtype; shape : int list}

(** A module containing functions to encode/decode an array chunk using a
    predefined set of codecs. *)
module Chain : sig
  (** A type representing a valid chain of codecs for
      decoding/encoding a Zarr array chunk. *)
  type t

  (** [create s c] returns a type representing a chain of codecs defined by
      chain [c] and chunk shape [s].

      @raise Bytes_to_bytes_invariant
        if [c] contains more than one bytes->bytes codec.
      @raise Invalid_transpose_order
        if [c] contains a transpose codec with invalid order array.
      @raise Invalid_zstd_level
        if [c] contains a Zstd codec whose compression level is invalid.
      @raise Invalid_sharding_chunk_shape
        if [c] contains a shardingindexed codec with an
        incorrect inner chunk shape. *)
  val create : int list -> codec list -> t

  (** [encode t x] computes the encoded byte string representation of
      array chunk [x]. *)
  val encode : t -> 'a Ndarray.t -> string

  (** [decode t repr x] decodes the byte string [x] using codec chain [t]
      and decoded representation type [repr]. *)
  val decode : t -> 'a array_repr -> string -> 'a Ndarray.t

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
module Make (IO : Types.IO) : sig

  (** [is_just_sharding t] is [true] if the codec chain [t] contains only
      the [sharding_indexed] codec. *)
  val is_just_sharding : Chain.t -> bool

  val partial_encode :
    Chain.t ->
    (Types.range list -> string list IO.t) ->
    (?append:bool -> (int * string) list -> unit IO.t) ->
    int ->
    'a array_repr ->
    (int list * 'a) list ->
    'a ->
    unit IO.t

  val partial_decode :
    Chain.t ->
    (Types.range list -> string list IO.t) ->
    int ->
    'a array_repr ->
    (int * int list) list ->
    'a ->
    (int * 'a) list IO.t
end
