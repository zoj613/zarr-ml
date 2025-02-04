module type IO = sig
  type 'a t
  val return : 'a -> 'a t
  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val map : ('a -> 'b) -> 'a t -> 'b t
  val return_unit : unit t
  val iter : ('a -> unit t) -> 'a list -> unit t
  val fold_left : ('acc -> 'a -> 'acc t) -> 'acc -> 'a list -> 'acc t
  val concat_map : ('a -> 'b list t) -> 'a list -> 'b list t
  module Infix : sig
    val (>>=) : 'a t -> ('a -> 'b t) -> 'b t
    val (>>|) : 'a t -> ('a -> 'b) -> 'b t
  end
  module Syntax : sig
    val (let*) : 'a t -> ('a -> 'b t) -> 'b t
    val (let+) : 'a t -> ('a -> 'b) -> 'b t
  end
end

type key = string
type range = int * int option
type value = string
type range_start = int
type prefix = string

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
  type 'a io
  val size : t -> key -> int io
  val get : t -> key -> value io
  val get_partial_values : t -> string -> range list -> value list io
  val set : t -> key -> value -> unit io
  val set_partial_values : t -> key -> ?append:bool -> (range_start * value) list -> unit io
  val erase : t -> key -> unit io
  val erase_prefix : t -> key -> unit io
  val list : t -> key list io
  val list_dir : t -> key -> (key list * prefix list) io
  val is_member : t -> key -> bool io
  val rename : t -> key -> key -> unit io
end
