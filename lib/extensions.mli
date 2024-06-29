module RegularGrid : sig
  type t
  val create : int array -> t
  val chunk_shape : t -> int array
  val grid_shape : t -> int array -> int array
  val indices : t -> int array -> int array list
  val index_coord_pair : t -> int array -> int array * int array
  val of_yojson : Yojson.Safe.t -> (t, string) result
  val to_yojson : t -> Yojson.Safe.t
end

type separator = Dot | Slash
(** A type representing the separator in an array chunk's key encoding.
    For example, [Dot] is "/", and is used to encode the chunk index
    [(0, 3, 5)] as [0/3/5]. *) 

module ChunkKeyEncoding : sig
  type t
  val create : separator -> t
  val encode : t -> int array -> string
  val of_yojson : Yojson.Safe.t -> (t, string) result
  val to_yojson : t -> Yojson.Safe.t
end

module Datatype : sig
  (** Data types as defined in the Zarr V3 specification *)

  type t =
    | Char
    | Int8
    | Uint8
    | Int16
    | Uint16
    | Int32
    | Int64
    | Float32
    | Float64
    | Complex32
    | Complex64
    | Int
    | Nativeint
  (** A type for the supported data types of a Zarr array. *)

  val of_kind : ('a, 'b) Bigarray.kind -> t
  val of_yojson : Yojson.Safe.t -> (t, string) result
  val to_yojson : t -> Yojson.Safe.t
end
