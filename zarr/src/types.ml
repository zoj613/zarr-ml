module type IO = sig
  type 'a t
  val return : 'a -> ('a, _) result t
  val error : 'e -> (_, 'e) result t
  val return_unit : (unit, _) result t
  val lift : ('a, 'b) result -> ('a, 'b) result t
  val bind : ('a, 'e) result t -> ('a -> ('b, 'e) result t) -> ('b, 'e) result t
  val map : ('a -> 'b) -> ('a, 'e) result t -> ('b, 'e) result t
  val fold_left : ((('a, _) result as 'r) -> 'b -> 'r t) -> 'r -> 'b list -> 'r t
  module Infix : sig
    val (>>=) : ('a, 'e) result t -> ('a -> ('b, 'e) result t) -> ('b, 'e) result t
    val (>>|) : ('a, 'e) result t -> ('a -> 'b) -> ('b, 'e) result t
  end
  module Syntax : sig
    val (let*) : ('a, 'e) result t -> ('a -> ('b, 'e) result t) -> ('b, 'e) result t 
    val (let+) : ('a, 'e) result t -> ('a -> 'b) -> ('b, 'e) result t
  end
end

type key = string
type range = int * int option
type value = string
type range_start = int
type prefix = string
type chunk_coord = int list

module type Store = sig
  (** The abstract store interface that stores should implement.

      The store interface defines a set of operations involving keys and values.
      In the context of this interface, a key is a Unicode string, where the final
      character is not a "/". In general, a value is a sequence of bytes.
      Specific stores may choose more specific storage formats, which must be
      stated in the specification of the respective store. 

      It is assumed that the store holds (key, value) pairs, with only one
      such pair for any given key. (i.e. a store is a mapping from keys to
      values). It is also assumed that keys are case sensitive, i.e., the keys
      “foo” and “FOO” are different. The store interface also defines some
      operations involving prefixes. In the context of this interface,
      a prefix is a string containing only characters that are valid for use
      in keys and ending with a trailing / character. *)
  type t
  type error
  type 'a io
  val size : t -> key -> (int, [> `Zarr of error ]) result io
  val get : t -> key -> (value, [> `Zarr of error ]) result io
  val get_partial_values : t -> string -> range list -> (value list, [> `Zarr of error ]) result io
  val set : t -> key -> value -> (unit, [> `Zarr of error ]) result io
  val set_partial_values : t -> key -> ?append:bool -> (range_start * value) list -> (unit, [> `Zarr of error ]) result io
  val erase : t -> key -> (unit, [> `Zarr of error ]) result io
  val erase_prefix : t -> key -> (unit, [> `Zarr of error ]) result io
  val list : t -> (key list, [> `Zarr of error ]) result io
  val list_dir : t -> key -> ((key list * prefix list), [> `Zarr of error ]) result io
  val is_member : t -> key -> (bool, [> `Zarr of error ]) result io
  val rename : t -> key -> key -> (unit, [> `Zarr of error ]) result io
end
