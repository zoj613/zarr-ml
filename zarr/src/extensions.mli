module RegularGrid : sig
  exception Grid_shape_mismatch
  type t
  val create : array_shape:int array -> int array -> t
  val chunk_shape : t -> int array
  val indices : t -> int array -> int array list
  val index_coord_pair : t -> int array -> int array * int array
  val ( = ) : t -> t -> bool
  val to_yojson : t -> Yojson.Safe.t
end

module ChunkKeyEncoding : sig
  type t
  val create : [< `Slash | `Dot > `Slash ] -> t
  val encode : t -> int array -> string
  val ( = ) : t -> t -> bool
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

  val ( = ) : t -> t -> bool
  val of_kind : 'a Ndarray.dtype -> t
  val of_yojson : Yojson.Safe.t -> (t, string) result
  val to_yojson : t -> Yojson.Safe.t
end
