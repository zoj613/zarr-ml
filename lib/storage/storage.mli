include Storage_intf.Interface

module MemoryStore : sig 
  (** An in-memory storage backend for Zarr V3 hierarchy. *)

  include S

  val create : unit -> t
  (** [create ()] returns a new In-memory Zarr V3 store. *)
end

module FilesystemStore : sig
  (** A local filesystem storage backend for a Zarr V3 hierarchy. *)

  include S

  val create
    : ?file_perm:Unix.file_perm -> string -> t
  (** [create ~file_perm path] returns a new filesystem Zarr V3 store. This
   *  operatioin fails if [path] is a directory that already exists. *)

  val open_store
    : ?file_perm:Unix.file_perm -> string -> (t, error) result
  (** [open_store ~file_perm path] returns an existing filesystem Zarr V3 store.
   * This operatioin fails if [path] is not a Zarr store path. *)

  val open_or_create
    : ?file_perm:Unix.file_perm -> string -> (t, error) result
  (** [open_or_create ~file_perm path] returns an existing filesystem store
   * and creates it if it does not exist at path [path]. See documentation
   * for {!open_store} and {!create} for more information. *)
end
