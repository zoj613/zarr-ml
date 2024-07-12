(** An array has an associated list of codecs. Each codec specifies a
    bidirectional transform (an encode transform and a decode transform).
    This module contains building blocks for creating and working with
    a chain of codecs. *)

module Ndarray = Owl.Dense.Ndarray.Generic

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
and 'a shard_chain = {
  a2a: arraytoarray list;
  a2b: arraytobytes;
  b2b: 'a list;
}

(** A type used to build a user-defined chain of codecs when creating a Zarr array. *)
type codec_chain = {
  a2a: arraytoarray list;
  a2b: arraytobytes;
  b2b: bytestobytes list;
}

(** The type of errors returned upon failure when an calling a function
  on a {!Chain} type. *)
type error =
  [ `Extension of string 
  | `Gzip of Ezgzip.error
  | `Transpose_order of int array * string
  | `Sharding of int array * int array * string ]

(** A module containing functions to encode/decode an array chunk using a
    predefined set of codecs. *)
module Chain : sig
  (** A type representing a valid chain of codecs for
      decoding/encoding a Zarr array chunk. *)
  type t

  (** [create r c] returns a type representing a chain of codecs defined by
      chain [c] and decoded array representation type [r]. *)
  val create :
    ('a, 'b) Util.array_repr -> codec_chain -> (t, [> error ]) result

  (** [default] returns the default codec chain that contains only
      the required codecs as defined in the Zarr Version 3 specification. *)
  val default : t

  (** [encode t x] computes the encoded byte string representation of
      array chunk [x]. Returns an error upon failure. *)
  val encode :
    t -> ('a, 'b) Ndarray.t -> (string, [> error ]) result

  (** [decode t repr x] decodes the byte string [x] using codec chain [t]
      and decoded representation type [repr]. Returns an error upon failure.*)
  val decode :
    t -> ('a, 'b) Util.array_repr -> string -> (('a, 'b) Ndarray.t, [> error]) result

  (** [x = y] returns true if chain [x] is equal to chain [y],
      and false otherwise. *)
  val ( = ) : t -> t -> bool

  (** [of_yojson x] returns a code chain of type {!t} from its json object
      representation. *)
  val of_yojson : Yojson.Safe.t -> (t, string) result

  (** [to_yojson x] returns a json object representation of codec chain [x]. *)
  val to_yojson : t -> Yojson.Safe.t
end
