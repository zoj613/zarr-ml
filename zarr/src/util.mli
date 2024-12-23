(** A finite map over integer array keys. *)
module ArrayMap : sig
  include Map.S with type key = int array
  val add_to_list : int array -> 'a -> 'a list t -> 'a list t
  (** [add_to_list k v map] is [map] with [k] mapped to [l] such that [l]
      is [v :: ArrayMap.find k map] if [k] was bound in [map] and [v] otherwise.*)
end

(** Result monad operator syntax. *)
module Result_syntax : sig
  val (let*) : ('a, 'e) result -> ('a -> ('b, 'e) result ) -> ('b, 'e) result
  val (let+) : ('a, 'e) result -> ('a -> 'b) -> ('b, 'e) result
end

val get_name : Yojson.Safe.t -> string
(** [get_name c] returns the name value of a JSON metadata extension point
    configuration of the form [{"name": value, "configuration": ...}],
    as defined in the Zarr V3 specification. *)

val prod : int array -> int
(** [prod x] returns the product of the elements of [x]. *)

val max : int array -> int
(** [max x] returns the maximum element of an integer array [x]. *)

val create_parent_dir : string -> int -> unit
(** [create_parent_dir f p] creates all the parent directories of file name
    [f] if they don't exist given file permissions [p]. *)

val sanitize_dir : string -> string
(** [sanitize_dir d] Chops off any trailing '/' in directory path [d]. *)
