module MemoryStore : sig
  (** An in-memory storage backend for Zarr V3 hierarchy. *)

  include Zarr.Storage.STORE with type 'a Deferred.t = 'a

  val create : unit -> t
  (** [create ()] returns a new In-memory Zarr store type. *)
end

(** A blocking I/O Zip file storage backend for a Zarr v3 hierarchy. *)
module ZipStore : sig include Zarr.Zip.S with type 'a Deferred.t = 'a end

module FilesystemStore : sig
  (** A local filesystem storage backend for a Zarr V3 hierarchy. *)

  include Zarr.Storage.STORE with type 'a Deferred.t = 'a

  val create : ?perm:Unix.file_perm -> string -> t
  (** [create ~perm dir] returns a new filesystem store.

      @raise Failure if [dir] is a directory that already exists.*)

  val open_store : ?perm:Unix.file_perm -> string -> t
  (** [open_store ~perm dir] returns an existing filesystem Zarr store.

      @raise Failure if [dir] is not a Zarr store path. *)
end
