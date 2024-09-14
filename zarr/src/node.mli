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

exception Node_invariant
(** raised when a node's invariants are violated. *)

exception Cannot_rename_root
(** raised when attempting to rename a root node. *)

module GroupNode : sig
  type t
  (** The type of a Group node. *)

  val root : t
  (** creates the root node *)

  val create : t -> string -> t
  (** [create p n] returns a group node with parent [p] and name [n].
      @raise Failure if node invariants are not satisfied. *)

  val ( / ) : t -> string -> t
  (** The infix operator alias of {!create} *)

  val of_path : string -> t 
  (** [of_path s] returns a node from string [s].
      @raise Failure if node invariants are not satisfied. *)

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

  val is_child_group : t -> t -> bool
  (** [is_child_group m n] Tests if group node [m] is a the immediate parent of
      group node [n]. Returns [true] when the test passes and [false] otherwise. *)

  val show : t -> string
  (** [show n] returns a string representation of a node type. *)

  val pp : Format.formatter -> t -> unit
  (** [pp fmt t] pretty prints a node type value. *)

  val rename : t -> string -> t
  (** [rename t s] returns a new group node with all properties of [t]
      but with its name changed to [s].

      @raise Node_invariant if [s] is invalid name.
      @raise Renaming_root if [t] is a root node.*)

end

module ArrayNode : sig
  type t
  (** The type of an array node. *)

  val create : GroupNode.t -> string -> t
  (** [create p n] returns an array node with parent [p] and name [n].
      @raise Failure if node invariants are not satisfied. *)

  val ( / ) : GroupNode.t -> string -> t
  (** The infix operator alias of {!ArrayNode.create} *)

  val root : t
  (** creates an array root node *)

  val of_path : string -> t
  (** [of_path s] returns an array node from string [s].
      @raise Failure if node invariants are not satisfied. *)

  val to_path : t -> string
  (** [to_path n] returns array node [n] as a string path. *)

  val name : t -> string
  (** [name n] returns the name of array node [n]. *)

  val parent : t -> GroupNode.t option
  (** [parent n] returns [Some p] where [p] is the parent group node of [n]
      or [None] if node [n] is a root node. *)

  val ( = ) : t -> t -> bool
  (** [x = y] returns [true] if nodes [x] and [y] are equal,
      and [false] otherwise. *)

  val ancestors : t -> GroupNode.t list
  (** [ancestors n] returns ancestor group nodes of [n]. *)

  val is_parent : t -> GroupNode.t -> bool
  (** [is_parent n g] returns [true] if group node [g] is the immediate
      parent of array node [n] and [false] otherwise. *)

  val to_key : t -> string
  (** [to_key n] converts a node's path to a key, as defined in the Zarr V3
      specification. *)

  val to_metakey : t -> string
  (** [to_prefix n] returns the metadata key associated with node [n],
      as defined in the Zarr V3 specification. *)

  val show : t -> string
  (** [show n] returns a string representation of a node type. *)

  val pp : Format.formatter -> t -> unit
  (** [pp fmt t] pretty prints a node type value. *)

  val rename : t -> string -> t
  (** [rename t s] returns a new node with all properties of [t]
      but with its name changed to [s].

      @raise Node_invariant if [s] is invalid name.
      @raise Renaming_root if [t] is a root node.*)
end
