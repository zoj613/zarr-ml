(** This module provides functionality for manipulating Zarr nodes.

    A Zarr V3 node is associated with either a group or an array.
    All nodes in a hierarchy have a name and a path. The root node does not
    have a name and is the empty string "". Except for the root node, each
    node in a hierarchy must have a name, which is a string of unicode code
    points. The following constraints apply to node names:
    - must not be the empty string ("").
    - must not include the character "/".
    - must not be a string composed only of period characters, e.g. "." or "..".
    - must not start with the reserved prefix "__".*)

type t
(** The type of a node. *)

type error =
  [ `Node_invariant of string ]
(** The error type for operations on the {!Node.t} type. It is returned by
    functions that create a {!Node.t} type when one or more of a Node's
    invariants are not satisfied as defined in the Zarr V3 specification.*)

val root : t
(** Returns the root node *)

val create : t -> string -> (t, [> error]) result
(** [create p n] returns a node with parent [p] and name [n]
    or an error of type {!error} if this operation fails. *)

val ( / ) : t -> string -> (t, [> error]) result
(** The infix operator alias of {!Node.create} *)

val of_path : string -> (t, [> error]) result
(** [of_path s] returns a node from string [s] or an error of
    type {!error} upon failure. *)

val to_path : t -> string
(** [to_path n] returns node [n] as a string path. *)

val name : t -> string
(** [name n] returns the name of node [n]. The root node does not have a
    name and thus the empty string [""] is returned if [n] is a root node. *)

val parent : t -> t option
(** [parent n] returns [Some p] where [p] is the parent node of [n]
    of [None] if node [n] is the root node. *)

val ( = ) : t -> t -> bool
(** [x = y] returns [true] if nodes [x] and [y] are equal,
    and [false] otherwise. *)

val ancestors : t -> t list
(** [ancestors n] returns ancestor nodes of [n] including the root node.
    The root node has no ancestors, thus this returns the empty list
    is called on a root node. *)

val to_key : t -> string
(** [to_key n] converts a node's path to a key, as defined in the Zarr V3
    specification. *)

val to_prefix : t -> string
(** [to_prefix n] converts a node's path to a prefix key, as defined
    in the Zarr V3 specification. *)

val to_metakey : t -> string
(** [to_prefix n] returns the metadata key associated with node [n],
    as defined in the Zarr V3 specification. *)

val is_parent : t -> t -> bool
(** [is_parent m n] Tests if node [n] is a the immediate parent of
    node [m]. Returns [true] when the test passes and [false] otherwise. *)
