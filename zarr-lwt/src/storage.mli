(** An Lwt-aware in-memory storage backend for Zarr v3 hierarchy. *)
module MemoryStore : sig include Zarr.Memory.S with type 'a Deferred.t = 'a Lwt.t end

(** An Lwt-aware Zip file storage backend for a Zarr v3 hierarchy. *)
module ZipStore : Zarr.Zip.S with type 'a Deferred.t = 'a Lwt.t

(** An Lwt-aware local filesystem storage backend for a Zarr V3 hierarchy. *)
module FilesystemStore : sig
  include Zarr.Storage.STORE with type 'a Deferred.t = 'a Lwt.t

  val create : ?perm:Unix.file_perm -> string -> t
  (** [create ~perm dir] returns a new filesystem store.

      @raise Failure if [dir] is a directory that already exists.*)

  val open_store : ?perm:Unix.file_perm -> string -> t
  (** [open_store ~perm dir] returns an existing filesystem Zarr store.

      @raise Failure if [dir] is not a Zarr store path. *)
end
