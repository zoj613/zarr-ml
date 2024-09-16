(** Supported data types for a Zarr array. *)
type _ dtype =
  | Char : char dtype
  | Bool : bool dtype
  | Int8 : int dtype
  | Uint8 : int dtype
  | Int16 : int dtype
  | Uint16 : int dtype
  | Int32 : int32 dtype
  | Int64 : int64 dtype
  | Uint64 : Stdint.uint64 dtype
  | Float32 : float dtype
  | Float64 : float dtype
  | Complex32 : Complex.t dtype
  | Complex64 : Complex.t dtype
  | Int : int dtype
  | Nativeint : nativeint dtype

type 'a t
(** The type for n-dimensional view of a Zarr array.*)

val dtype_size : 'a dtype -> int
(** [dtype_size kind] returns the size in bytes of data type [kind].*)

val create : 'a dtype -> int array -> 'a -> 'a t
(** [create k s v] creates an N-dimensional array with data_type [k],
    shape [s] and fill value [v].*)

val init : 'a dtype -> int array -> (int -> 'a) -> 'a t
(** [init k s f] creates an N-dimensional array with data_type [k],
    shape [s] and every element value is assigned using function [f].*)

val data_type : 'a t -> 'a dtype
(** [data_type x] returns the data_type associated with [x].*)

val size : 'a t -> int
(** [size x] is the total number of elements of [x].*)

val ndims : 'a t -> int
(** [ndims x] is the number of dimensions of [x].*)

val shape : 'a t -> int array
(** [shape x] returns an array with the size of each dimension of [x].*)

val byte_size : 'a t -> int
(** [byte_size x] is the total size occupied by the byte sequence of elements
    of [x]. *)

val to_array : 'a t -> 'a array 
(** [to_array x] returns the data of [x] as a 1-d array of type determined by
    {!data_type}. Note that data is not copied, so if the caller modifies the
    returned array, the changes will be reflected in [x].*)

val of_array : 'a dtype -> int array -> 'a array -> 'a t
(** [of_array k s x] creates an n-dimensional array of shape [s] and data_type
    [k] using elements of [x]. Note that the data is not copied, so the
    caller must ensure not to modify [x] afterwards.*)

val get : 'a t -> int array -> 'a
(** [get x c] returns element of [x] at coordinate [c].*)

val set : 'a t -> int array -> 'a -> unit
(** [set x c v] sets coordinate [c] of [x] to value [v].*)

val iteri : (int -> 'a -> unit) -> 'a t -> unit
(** Same as {!iter} but the function is applied to the index of the element as
    first argument and the element value as the second.*)

val fill : 'a t -> 'a -> unit
(** [fill x v] replaces all elements of [x] with value [v].*)

val map : ('a -> 'a) -> 'a t -> 'a t
(** [map f x] applies function [f] to all elements of [x] and builds an
    n-dimensional array of same shape and data_type as [x] with the result.*)

val iter : ('a -> unit) -> 'a t -> unit
(** [iteri f x] applies function [f] to all elements of [x] in row-major order.*)

val equal : 'a t -> 'a t -> bool
(** [equal x y] is [true] iff [x] and [y] are equal, else [false].*)

val transpose : ?axis:int array -> 'a t -> 'a t
(** [transpose o x] permutes the axes of [x] according to [o].*)

val to_bigarray : 'a t -> ('a, 'b) Bigarray.kind -> ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t
(** [to_bigarray x] returns a C-layout Bigarray representation of [x]. *)

val of_bigarray : ('a, 'b, 'c) Bigarray.Genarray.t -> 'a t
(** [of_bigarray x] returns an N-dimensional array representation of [x].*)

module Indexing : sig
  (** A module housing functions for creating and manipulating indices and
      slices for working with Zarr arrays. *)

  type index =
    | I of int
    | L of int array
    | R of int array

  val slice_of_coords : int array list -> index array
  (** [slice_of_coords c] takes a list of array coordinates and returns
      a slice corresponding to the coordinates. Elements of each slice
      variant are sorted in increasing order.*)
      
  val coords_of_slice : index array -> int array -> int array array
  (** [coords_of_slice s shp] returns an array of coordinates given
      a slice [s] and array shape [shp]. *)

  val cartesian_prod : int list list -> int list list
  (** [cartesian_prod ll] returns a cartesian product of the elements of
      list [ll]. It is mainly used to generate a C-order of chunk indices
      in a regular Zarr array grid. *)

  val slice_shape : index array -> int array -> int array
  (** [slice_shape s shp] returns the shape of slice [s] within an array
      of shape [shp]. *)
end
