type error =
  [ `Extension of string ]

module RegularGrid : sig
  type t
  val create : array_shape:int array -> int array -> (t, [> error]) result
  val chunk_shape : t -> int array
  val grid_shape : t -> int array -> int array
  val indices : t -> int array -> int array list
  val index_coord_pair : t -> int array -> int array * int array
  val ( = ) : t -> t -> bool
  val of_yojson : Yojson.Safe.t -> (t, string) result
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
  val of_kind : ('a, 'b) Bigarray.kind -> t
  val of_yojson : Yojson.Safe.t -> (t, string) result
  val to_yojson : t -> Yojson.Safe.t
end

type tf_error =
  [ `Store_read of string
  | `Store_write of string ]

module type STF = sig
  type t
  val get : t -> string -> (string, [> tf_error]) result
  val set : t -> string -> string -> unit
  val erase : t -> string -> unit
end

module StorageTransformers : sig
  type transformer =
    | Identity
  type t = transformer list

  val default : t
  val get :
    (module STF with type t = 'a) -> 'a -> t -> string -> (string, [> tf_error ]) result
  val set :
    (module STF with type t = 'a) -> 'a -> t -> string -> string -> unit
  val erase :
    (module STF with type t = 'a) -> 'a -> t -> string -> unit
  val to_yojson : t -> Yojson.Safe.t
  val of_yojson : Yojson.Safe.t -> (t, string) result
end
