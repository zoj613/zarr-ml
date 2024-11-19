module Deferred : Zarr.Types.Deferred with type 'a t = 'a

(** A blocking I/O in-memory storage backend for Zarr v3 hierarchy. *)
module MemoryStore : Zarr.Memory.S with module Deferred = Deferred

(** A blocking I/O Zip file storage backend for a Zarr v3 hierarchy. *)
module ZipStore : Zarr.Zip.S with module Deferred = Deferred

(** A blocking I/O local filesystem storage backend for a Zarr v3 hierarchy. *)
module FilesystemStore : sig
  include Zarr.Storage.STORE with module Deferred = Deferred

  val create : ?perm:int -> string -> t
  (** [create ~perm dir] returns a new filesystem store.

      @raise Failure if [dir] is a directory that already exists.*)

  val open_store : ?perm:int -> string -> t
  (** [open_store ~perm dir] returns an existing filesystem Zarr store.

      @raise Failure if [dir] is not a Zarr store path. *)
end
