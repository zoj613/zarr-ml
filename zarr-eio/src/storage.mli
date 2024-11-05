(** An in-memory storage backend for Zarr V3 hierarchy. *)
module MemoryStore : sig include Zarr.Memory.S with type 'a Deferred.t = 'a end

(** An Eio-aware Zip file storage backend for a Zarr v3 hierarchy. *)
module ZipStore : sig include Zarr.Zip.S with type 'a Deferred.t = 'a end

module FilesystemStore : sig
  (** A local filesystem storage backend for a Zarr V3 hierarchy. *)

  include Zarr.Storage.STORE with type 'a Deferred.t = 'a

  val create : ?perm:Eio.File.Unix_perm.t -> env:<fs : Eio.Fs.dir_ty Eio.Path.t; ..> -> string -> t
  (** [create ~perm ~env dir] returns a new filesystem store.

      @raise Failure if [dir] is a directory that already exists.*)

  val open_store : ?perm:Eio.File.Unix_perm.t -> env:<fs : Eio.Fs.dir_ty Eio.Path.t; ..> -> string -> t
  (** [open_store ~perm ~env dir] returns an existing filesystem Zarr store.

      @raise Failure if [dir] is a file and not a Zarr store path. *)
end
