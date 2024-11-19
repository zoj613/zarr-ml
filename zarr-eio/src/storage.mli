module Deferred : Zarr.Types.Deferred with type 'a t = 'a

(** An Eio-aware in-memory storage backend for Zarr v3 hierarchy. *)
module MemoryStore : Zarr.Memory.S with module Deferred = Deferred

(** An Eio-aware Zip file storage backend for a Zarr v3 hierarchy. *)
module ZipStore : Zarr.Zip.S with module Deferred = Deferred

(** An Eio-aware local filesystem storage backend for a Zarr v3 hierarchy. *)
module FilesystemStore : sig
  include Zarr.Storage.STORE with module Deferred = Deferred

  val create : ?perm:Eio.File.Unix_perm.t -> env:<fs : Eio.Fs.dir_ty Eio.Path.t; ..> -> string -> t
  (** [create ~perm ~env dir] returns a new filesystem store.

      @raise Failure if [dir] is a directory that already exists.*)

  val open_store : ?perm:Eio.File.Unix_perm.t -> env:<fs : Eio.Fs.dir_ty Eio.Path.t; ..> -> string -> t
  (** [open_store ~perm ~env dir] returns an existing filesystem Zarr store.

      @raise Failure if [dir] is a file and not a Zarr store path. *)
end
